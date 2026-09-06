## 1. Executive Summary

In enterprise ERP systems, a single primary transactional state change frequently triggers multiple automated secondary operations across inventory and financial ledgers.

### 📦 Key Case Study: Sales Order Fulfillment (`transaction_sales_order`)
When a warehouse manager approves the dispatch of goods by updating a Sales Order's `status` from `'APPROVED'` (or `'PENDING'`) to `'SHIPPED'` (via `PATCH /transaction_sales_order/{id}`), the backend engine must automatically execute 3 critical cascading actions within a single atomic database transaction:

1. **Inventory Stock Deduction**: Automatically query line items from `transaction_sales_order_item` and deduct the corresponding quantity (`qty`) from the finished goods lot inventory in `transaction_product_lot`.
2. **Accounts Receivable (AR) Invoicing**: Automatically create an AR invoice draft in `transaction_account_receivable` matching the Sales Order's total amount and customer details.
3. **General Ledger (GL) Auto-Posting**: Automatically insert balanced journal entries into `transaction_general_ledger` (Debit Cost of Goods Sold / Credit Finished Goods Inventory).

Currently, `flx-nocode-api` handles standard single-entity and master-detail CRUD operations cleanly, but lacks a **declarative, engine-level trigger/hook mechanism** in entity JSON configurations (`action_triggers`, `on_status_change`) to execute these multi-table cascading workflows automatically on the backend.

---

## 2. Why `pre_process` and `post_process` Are Insufficient for ERP

In `flx-nocode-api`, entity configuration JSONs currently support `pre_process` and `post_process` fields (which execute raw SQL strings before or after a database operation):

```json
{
  "table": "transaction_sales_order",
  "put": {
    "post_process": "UPDATE transaction_product_lot SET qty = qty - {qty} WHERE product_id = {product_id}"
  }
}
```

However, relying on `post_process` fails in complex ERP scenarios due to 4 fundamental architectural limitations:

1. **Lack of Conditional Execution (Runs Unconditionally)**:
   - Raw SQL in `post_process` executes **every single time** a POST, PUT, or PATCH request is made, regardless of which field was modified.
   - *Example Problem*: If a user updates only the shipping address via `PATCH /transaction_sales_order/105` with `{"shipping_address": "Jakarta"}`, the `post_process` SQL query above will **wrongfully execute and deduct stock again**. `post_process` cannot evaluate conditional rules such as *"Only execute when `status` changes to `'SHIPPED'`"*.

2. **Inability to Dynamically Loop Over Relational Detail Arrays (BOM / Line Items)**:
   - A static SQL string in `post_process` cannot perform per-row dynamic iteration over relational child items.
   - *Example Problem*: In Sales Order shipment, stock must be deducted for N different line items (e.g., 5 items in a Sales Order). A static `UPDATE` query cannot dynamically iterate over child rows in `transaction_sales_order_item` and deduct matching stock in `transaction_product_lot` without resorting to complex, database-specific Stored Procedures.

3. **Database Portability & Maintenance Overhead**:
   - Forcing complex multi-table logic into `post_process` requires writing DB-specific stored procedures (`JSON_TABLE` in MySQL vs `json_to_recordset` in PostgreSQL). This completely breaks Flexurio's cross-database no-code portability promise (SQLite, MySQL, PostgreSQL, MSSQL).

4. **Lack of Payload Variable Extraction for Indirect Data**:
   - `post_process` can only substitute direct fields present in the incoming HTTP request body (e.g., `{id}`). When receiving a clean status update payload `{"status": "SHIPPED"}`, variables like `{product_id}` and `{qty}` do not exist in the request body, causing `post_process` SQL placeholders to fail or evaluate to `NULL`.

---

## 3. Why Client HTTP Body Payloads Are Strongly Discouraged for Status Triggers

### 📋 Case Example: Sales Order Shipment & Product Lot Deduction

When a warehouse manager approves a Sales Order shipment, the client sends a clean status update request:

```http
PATCH /transaction_sales_order/105 HTTP/1.1
Host: api.flexurio.com
Content-Type: application/json

{
  "status": "SHIPPED"
}
```

If developers attempt to solve this via `post_process` or client body payloads, they are forced to mandate sending detail items inside the status patch body:

```http
❌ DISCOURAGED ANTI-PATTERN PAYLOAD:
PATCH /transaction_sales_order/105 HTTP/1.1
Content-Type: application/json

{
  "status": "SHIPPED",
  "items": [
    { "product_id": 12, "qty_shipped": 5 },
    { "product_id": 14, "qty_shipped": 10 }
  ]
}
```

Passing detail items/quantities in client HTTP body payloads during status updates is considered a critical anti-pattern because:

1. **Security & Data Tampering Risk**: Client-supplied payloads can be intercepted or manipulated (e.g. changing `qty_shipped` to 0 or altering `product_id` to bypass stock deduction). Backend status triggers must query and consume canonical database relations directly (`transaction_sales_order_item`).
2. **Violation of Single Source of Truth (SSOT)**: Transaction lines (SO/PO items) already exist canonically in database detail tables. Forcing frontend/mobile clients to re-fetch and re-transmit item arrays in request bodies introduces data desynchronization risks.

---

## 4. Why Sequential FE Multi-Endpoint Calls Are Strongly Discouraged

If developers attempt to orchestrate cascading ERP workflows by firing multiple HTTP requests sequentially from the Frontend (FE) or Mobile App:

```http
1. PATCH /transaction_sales_order/105             (Update status -> SHIPPED)
2. PUT   /transaction_product_lot/1                (Deduct stock Product A)
3. PUT   /transaction_product_lot/2                (Deduct stock Product B)
4. POST  /transaction_account_receivable           (Create AR Invoice)
5. POST  /transaction_general_ledger               (Create GL Journal Voucher)
```

This approach is considered a dangerous anti-pattern due to:

1. **Loss of Transactional Atomicity (No ACID Guarantee)**: If the client loses internet connection, encounters a network timeout, or crashes at Step 3, the database is left in a **corrupted, half-processed state** (e.g., status is `SHIPPED`, Product A stock is deducted, but Product B stock is unchanged, AR invoice is missing, and GL is unposted).
2. **Security & Data Manipulation Risk**: Because the frontend directly calls update endpoints for downstream ledgers (`PUT /transaction_product_lot` or `POST /transaction_general_ledger`), a malicious client/user can **skip calling specific endpoints entirely** (e.g., send the `SHIPPED` status patch to deliver goods, but deliberately bypass calling the API to deduct stock or create the AR invoice).
3. **High Network Latency & Overhead**: Making 5+ round-trip HTTP requests over mobile/cellular networks introduces severe UI lag and poor User Experience (UX) compared to a single backend atomic transaction.
4. **Complex Frontend Error Handling & Compensation Logic**: If Step 4 fails, the frontend must manually write rollback logic (e.g., reverting status to `PENDING` and restoring stock). Hand-writing manual rollbacks on the client is error-prone and insecure.

---

## 5. Entity Schema & Data Flow Visualisation

Below are the 5 canonical database tables involved in the `PATCH /transaction_sales_order/105` shipment event:

### 1. `transaction_sales_order` (Header Document - Trigger Source)

| id (`int`) | so_number (`varchar`) | customer_id (`int`) | so_date (`varchar`) | total_net (`int`) | discount_pct (`int`) | ⚡ status (`varchar`) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 105 | SO/2026/09/0001 | 3 | 2026-09-05 | 250000 | 0 | **SHIPPED** *(Patched)* |

### 2. `transaction_sales_order_item` (Relational Source Items)

| id (`int`) | sales_order_id (`int`) | so_number (`varchar`) | product_id (`int`) | product_name (`varchar`) | qty (`int`) | unit_price (`int`) | subtotal (`int`) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 10 | 105 | SO/2026/09/0001 | 12 | Paracetamol 500mg | **5** | 10000 | 50000 |
| 11 | 105 | SO/2026/09/0001 | 14 | Amoxicillin 500mg | **10** | 20000 | 200000 |

### 3. `transaction_product_lot` (Inventory Stock - Deducted)

| id (`int`) | product_id (`int`) | lot_number (`varchar`) | qty Before (`int`) | ⚡ qty After Trigger (`int`) | status (`varchar`) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | 12 | LOT-2026-A1 | **100** | **95** *(Deducted 5)* | AVAILABLE |
| 2 | 14 | LOT-2026-B2 | **50** | **40** *(Deducted 10)* | AVAILABLE |

### 4. `transaction_account_receivable` (AR Invoice - Auto Generated Draft)

| id (`int`) | faktur_no (`varchar`) | customer_id (`int`) | faktur_date (`varchar`) | due_date (`varchar`) | total_receivable (`int`) | status (`varchar`) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 50 | INV/2026/09/0050 | 3 | 2026-09-05 | 2026-10-05 | **250000** | UNPAID |

### 5. `transaction_general_ledger_line` (GL Journal Entries - Auto Posted)

| id (`int`) | voucher_id (`int`) | account_code (`varchar`) | account_name (`varchar`) | debit (`int`) | credit (`int`) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | 201 | 1103 | Accounts Receivable (AR) | **250000** | 0 |
| 2 | 201 | 4101 | Sales Revenue | 0 | **250000** |
| 3 | 201 | 5101 | Cost of Goods Sold (COGS) | **150000** | 0 |
| 4 | 201 | 1104 | Finished Goods Inventory | 0 | **150000** |

---

## 6. Expected Benefits & ERP Impact

1. **Fully Automated P2P (Procure-to-Pay) & O2C (Order-to-Cash)**: Seamless 1-click PO/SO confirmation with automatic ledger & inventory updates.
2. **Prevent Data Tampering**: Enforces Single Source of Truth (SSOT) at the engine level.
3. **100% ACID Guarantee**: Cascading triggers run inside the same database transaction scope, ensuring complete financial rollbacks on error.