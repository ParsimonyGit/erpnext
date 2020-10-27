from requests.exceptions import HTTPError

import frappe
from erpnext.erpnext_integrations.doctype.shopify_log.shopify_log import make_shopify_log
from erpnext.erpnext_integrations.doctype.shopify_settings.shopify_settings import API_VERSION, get_headers, get_shopify_url
from frappe.utils import get_request_session, now


def sync_payout_from_shopify():
	"""
	Pull and sync payouts from Shopify Payments transactions with existing orders
	"""

	shopify_settings = frappe.get_single("Shopify Settings")
	if not shopify_settings.enable_shopify:
		return

	url = get_shopify_url(f"admin/api/{API_VERSION}/shopify_payments/payouts.json", shopify_settings)
	session = get_request_session()
	params = {
		"date_min": shopify_settings.last_sync_datetime
	}

	try:
		res = session.get(url, params=params, headers=get_headers(shopify_settings))
		res.raise_for_status()
	except HTTPError as e:
		error_message = res.json().get("errors", e)
		make_shopify_log(status="Warning", exception=error_message)
	except Exception as e:
		make_shopify_log(status="Error", exception=e)
		return

	payouts = res.json().get("payouts")

	for payout in payouts:
		# TODO: sync payout
		"""
		{
			"id": 623721858,
			"status": "paid",
			"date": "2012-11-12",
			"currency": "USD",
			"amount": "41.90",
			"summary": {
				"adjustments_fee_amount": "0.12",
				"adjustments_gross_amount": "2.13",
				"charges_fee_amount": "1.32",
				"charges_gross_amount": "44.52",
				"refunds_fee_amount": "-0.23",
				"refunds_gross_amount": "-3.54",
				"reserved_funds_fee_amount": "0.00",
				"reserved_funds_gross_amount": "0.00",
				"retried_payouts_fee_amount": "0.00",
				"retried_payouts_gross_amount": "0.00"
			}
		}
		"""

		pass

	shopify_settings.last_sync_datetime = now()
	shopify_settings.save()
