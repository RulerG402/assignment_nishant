"""Unit tests: masking, standardisation, source contracts and config helpers."""

import pandas as pd
import pytest

from nammamart import config, masking, sources
from nammamart import standardise as std


class TestMasking:
    def test_mask_email_keeps_two_chars_and_domain(self):
        assert masking.mask_email("test.user1@example.com") == "te***@example.com"

    @pytest.mark.parametrize("bad", ["N/A", "arjun.rao307example.com", "prakash@example", "", None])
    def test_mask_email_returns_none_for_invalid(self, bad):
        assert masking.mask_email(bad) is None

    def test_mask_phone_keeps_first_and_last_two_digits(self):
        assert masking.mask_phone("9000000021") == "90******21"

    @pytest.mark.parametrize("bad", ["996292312", "9804341", "98a4349021", None])
    def test_mask_phone_returns_none_for_invalid(self, bad):
        assert masking.mask_phone(bad) is None

    def test_mask_name_first_name_and_initial(self):
        assert masking.mask_name("Priya  Hegde") == "Priya H."
        assert masking.mask_name("Madonna") == "Madonna"
        assert masking.mask_name("   ") is None

    def test_mask_customers_drops_raw_pii_columns(self):
        frame = pd.DataFrame({"customer_id": ["C1"], "full_name": ["Priya Hegde"],
                              "email": ["test@example.com"], "phone": ["9000000021"]})
        masked = masking.mask_customers(frame)
        assert {"full_name", "email", "phone"}.isdisjoint(masked.columns)
        assert masked.loc[0, "email_masked"] == "te***@example.com"
        assert frame.loc[0, "email"] == "test@example.com"  # input untouched


class TestStandardise:
    def test_payment_mode_aliases(self):
        raw = pd.Series(["upi", "UPI ", "Cashh", "cod", "CARD", "Wallet", "bitcoin", ""])
        assert std.payment_mode(raw).tolist()[:6] == ["UPI", "UPI", "Cash on Delivery", "Cash on Delivery",
                                                      "Card", "Wallet"]
        assert std.payment_mode(raw).iloc[6] == "bitcoin"
        assert pd.isna(std.payment_mode(raw).iloc[7])

    def test_strict_timestamps(self):
        parsed = std.to_timestamp(pd.Series(["2026-09-01 10:00:00", "N/A", "2026-13-05 11:00:00",
                                             "31/09/2026 10:00", ""]))
        assert parsed.notna().tolist() == [True, False, False, False, False]

    def test_is_canonical_flags_case_and_whitespace(self):
        raw = pd.Series(["Silver", "SILVER", "Silver "])
        assert std.is_canonical(raw, std.loyalty_tier(raw)).tolist() == [True, False, False]


class TestSources:
    def test_reads_everything_as_text_and_keeps_blanks(self, data_dir):
        frame = sources.read_master(data_dir, "customers")
        assert len(frame) == 405
        assert (frame["city"] == "").sum() == 6
        assert frame["_row_num"].tolist()[:3] == [1, 2, 3]

    def test_renamed_column_fails_loudly(self, scratch_dataset):
        path = scratch_dataset / "orders.csv"
        text = path.read_text(encoding="utf-8")
        path.write_text(text.replace("order_amount", "total", 1), encoding="utf-8")
        with pytest.raises(sources.SchemaContractError, match=r"missing=\['order_amount'\].*unexpected=\['total'\]"):
            sources.read_orders(scratch_dataset)

    def test_missing_file_raises(self, scratch_dataset):
        (scratch_dataset / "products.csv").unlink()
        with pytest.raises(FileNotFoundError):
            sources.read_master(scratch_dataset, "products")

    def test_orders_stack_backfill_then_increment(self, data_dir):
        stacked = sources.read_orders(data_dir)
        assert len(stacked) == 2412 + 160
        assert stacked["_priority"].iloc[0] == 0 and stacked["_priority"].iloc[-1] == 20261001
        assert len(sources.read_orders(data_dir, include_increments=False)) == 2412
        assert len(sources.read_orders(data_dir, include_increments=False, include_base=False)) == 0


class TestConfig:
    @pytest.mark.parametrize("bad", ["../..", "../../dags", "/etc"])
    def test_source_dir_refuses_escape(self, bad):
        with pytest.raises(ValueError):
            config.source_dir(bad)

    def test_safe_run_id(self):
        assert config.safe_run_id("manual__2026-10-20T09:00:00+00:00") == "manual__2026-10-20T09_00_00_00_00"

    def test_increment_cutoff_from_file_name(self, data_dir):
        sources_ = config.order_sources(data_dir)
        assert [s["file"] for s in sources_] == ["orders.csv", "orders_2026-10-01_increment.csv"]
        assert str(sources_[1]["cutoff"]) == "2026-10-01 23:59:59"
