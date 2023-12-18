import frappe


def execute():
	purchase_receipts = frappe.get_all(
		"Purchase Receipt",
		filters={
			"docstatus": 1,
			"is_internal_supplier": 1,
			"inter_company_reference": ["is", "set"]
		},
		fields=["inter_company_reference", "name"]
	)

	for pr in purchase_receipts:
		sales_order_list = frappe.get_all(
			"Delivery Note Item",
			filters={"parent": pr.get("inter_company_reference")},
			pluck="against_sales_order"
		)

		for so in sales_order_list:
			po_reference = frappe.db.get_value("Sales Order", so, ["po_no"])
			if po_reference:
				po_details = frappe.get_all("Purchase Order Item", filters={"parent": po_reference}, fields=["name", "item_code"])
				pr_details = frappe.get_all("Purchase Receipt Item", filters={"parent": pr.get("name")}, fields=["name", "item_code"])
				for po in po_details:
					for pr_detail in pr_details:
						if pr_detail.item_code == po.item_code:
							frappe.db.set_value("Purchase Receipt Item", pr_detail.get("name"), {"purchase_order": po_reference, "purchase_order_item": po.get("name")})
