resource "google_billing_budget" "portfolio_guardrail" {
  count = var.enable_budget && var.billing_account != "" ? 1 : 0

  billing_account = var.billing_account
  display_name    = "${var.project_id} portfolio budget"

  amount {
    specified_amount {
      currency_code = "USD"
      units         = var.monthly_budget_usd
    }
  }

  threshold_rules {
    threshold_percent = 0.50
  }

  threshold_rules {
    threshold_percent = 0.80
  }

  threshold_rules {
    threshold_percent = 1.00
  }

  threshold_rules {
    threshold_percent = 1.00
    spend_basis       = "FORECASTED_SPEND"
  }
}
