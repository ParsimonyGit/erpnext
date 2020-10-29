import shopify
from shopify import ShopifyResource

import frappe
from erpnext.erpnext_integrations.doctype.shopify_log.shopify_log import make_shopify_log
from frappe.utils import now


class Payouts(ShopifyResource):
	# temporary class until Shopify adds it to their library
	_prefix_source = "/shopify_payments/"


def sync_payout_from_shopify():
	"""
	Pull and sync payouts from Shopify Payments transactions with existing orders
	"""

	shopify_settings = frappe.get_single("Shopify Settings")
	if not shopify_settings.enable_shopify:
		return

	with shopify_settings.get_shopify_session(temp=True):
		try:
			payouts = Payouts.find(
				date_min=shopify_settings.last_sync_datetime
			)
		except Exception as e:
			make_shopify_log(status="Error", exception=e, rollback=True)
			return

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
