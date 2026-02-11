# SQL Queries Reference Guide
## Month-End Close Automation System

This document contains key SQL queries used in the month-end close automation system, demonstrating proficiency in database operations for financial data.

---

## 📊 Database Schema

### Tables Created:
1. **chart_of_accounts** - Master list of GL accounts
2. **transactions** - All accounting transactions (journal entries)
3. **general_ledger** - Monthly aggregated account balances
4. **bank_statements** - Bank transaction records
5. **reconciliations** - Bank reconciliation results
6. **audit_log** - Complete audit trail

---

## 🔍 Sample SQL Queries

### 1. Trial Balance Query
```sql
SELECT 
    c.account_id,
    c.account_name,
    c.account_type,
    COALESCE(g.total_debits, 0) as total_debits,
    COALESCE(g.total_credits, 0) as total_credits,
    COALESCE(g.ending_balance, 0) as ending_balance,
    CASE 
        WHEN COALESCE(g.ending_balance, 0) > 0 
        THEN COALESCE(g.ending_balance, 0)
        ELSE 0 
    END as debit_balance,
    CASE 
        WHEN COALESCE(g.ending_balance, 0) < 0 
        THEN -COALESCE(g.ending_balance, 0)
        ELSE 0 
    END as credit_balance
FROM chart_of_accounts c
LEFT JOIN general_ledger g ON c.account_id = g.account_id
    AND g.period_year = 2026
    AND g.period_month = 1
WHERE c.is_active = 1
ORDER BY c.account_id;
```

**Business Purpose:** Generate trial balance to verify debits = credits before closing the books.

---

### 2. Bank Reconciliation Query (Outstanding Items)
```sql
-- Outstanding items: In books but not cleared at bank
SELECT 
    t.transaction_id,
    t.transaction_date,
    t.description,
    t.debit - t.credit as net_amount
FROM transactions t
WHERE t.account_id = '1000'
AND NOT EXISTS (
    SELECT 1 FROM bank_statements b
    WHERE b.transaction_id = t.transaction_id
)
ORDER BY t.transaction_date;
```

**Business Purpose:** Identify deposits in transit and outstanding checks for bank reconciliation.

---

### 3. Income Statement Query
```sql
SELECT 
    SUM(CASE WHEN c.account_type = 'Revenue' THEN t.credit ELSE 0 END) as total_revenue,
    SUM(CASE WHEN c.account_type = 'Expense' THEN t.debit ELSE 0 END) as total_expenses,
    SUM(CASE WHEN c.account_type = 'Revenue' THEN t.credit ELSE 0 END) -
    SUM(CASE WHEN c.account_type = 'Expense' THEN t.debit ELSE 0 END) as net_income
FROM transactions t
JOIN chart_of_accounts c ON t.account_id = c.account_id
WHERE strftime('%Y-%m', t.transaction_date) = '2026-01';
```

**Business Purpose:** Calculate monthly net income for financial reporting.

---

### 4. Balance Sheet Query
```sql
SELECT 
    SUM(CASE WHEN c.account_type = 'Asset' 
        THEN t.debit - t.credit ELSE 0 END) as total_assets,
    SUM(CASE WHEN c.account_type = 'Liability' 
        THEN t.credit - t.debit ELSE 0 END) as total_liabilities,
    SUM(CASE WHEN c.account_type = 'Equity' 
        THEN t.credit - t.debit ELSE 0 END) as total_equity
FROM transactions t
JOIN chart_of_accounts c ON t.account_id = c.account_id
WHERE t.transaction_date <= '2026-01-31';
```

**Business Purpose:** Generate balance sheet snapshot at month-end.

---

### 5. Data Quality Check - Unbalanced Transactions
```sql
SELECT 
    transaction_id,
    SUM(debit) as total_debits,
    SUM(credit) as total_credits,
    ABS(SUM(debit) - SUM(credit)) as variance
FROM transactions
GROUP BY transaction_id
HAVING ABS(SUM(debit) - SUM(credit)) > 0.01
ORDER BY variance DESC;
```

**Business Purpose:** Identify data quality issues where debits ≠ credits.

---

### 6. Account Activity Detail
```sql
SELECT 
    t.transaction_date,
    t.transaction_id,
    t.description,
    t.debit,
    t.credit,
    t.debit - t.credit as net_amount,
    SUM(t.debit - t.credit) OVER (
        ORDER BY t.transaction_date, t.transaction_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_balance
FROM transactions t
WHERE t.account_id = '1000'
ORDER BY t.transaction_date, t.transaction_id;
```

**Business Purpose:** Detailed ledger with running balance for audit trail.

---

### 7. Monthly GL Posting Aggregation
```sql
INSERT OR REPLACE INTO general_ledger 
(account_id, period_year, period_month, total_debits, total_credits, ending_balance)
SELECT 
    t.account_id,
    CAST(strftime('%Y', t.transaction_date) AS INTEGER) as period_year,
    CAST(strftime('%m', t.transaction_date) AS INTEGER) as period_month,
    SUM(t.debit) as total_debits,
    SUM(t.credit) as total_credits,
    SUM(t.debit) - SUM(t.credit) as ending_balance
FROM transactions t
WHERE strftime('%Y', t.transaction_date) = '2026'
AND strftime('%m', t.transaction_date) = '01'
GROUP BY t.account_id;
```

**Business Purpose:** Aggregate transactions into monthly GL balances for reporting.

---

### 8. Bank-Only Items Query
```sql
-- Items in bank statement but not in books (fees, interest)
SELECT 
    b.transaction_date,
    b.description,
    b.amount,
    b.statement_reference
FROM bank_statements b
WHERE b.transaction_id NOT IN (
    SELECT transaction_id FROM transactions
)
ORDER BY b.transaction_date;
```

**Business Purpose:** Identify bank fees, interest, and other items requiring journal entries.

---

### 9. Audit Trail Query
```sql
SELECT 
    audit_id,
    event_timestamp,
    event_type,
    table_name,
    description,
    user_name
FROM audit_log
WHERE event_timestamp >= date('now', '-30 days')
ORDER BY event_timestamp DESC
LIMIT 100;
```

**Business Purpose:** Compliance and tracking of all database changes.

---

### 10. Transaction Validation - Orphaned Records
```sql
-- Find transactions with invalid account references
SELECT 
    t.transaction_id,
    t.account_id,
    t.description,
    t.debit + t.credit as amount
FROM transactions t
WHERE NOT EXISTS (
    SELECT 1 FROM chart_of_accounts c
    WHERE c.account_id = t.account_id
)
ORDER BY t.transaction_date;
```

**Business Purpose:** Data integrity check for referential integrity.

---

## 🎯 Advanced SQL Techniques Demonstrated

### Window Functions
- **Running Balance:** Calculate cumulative balance using SUM() OVER()
- **Period-over-Period:** Compare current month vs prior month

### Aggregations
- **GROUP BY** with multiple dimensions (account, period)
- **CASE** statements for conditional aggregation
- **COALESCE** for handling NULLs in calculations

### Joins
- **LEFT JOIN** for trial balance (include all accounts)
- **INNER JOIN** for validated data only
- **NOT EXISTS** for reconciliation matching

### Subqueries
- Correlated subqueries for outstanding items
- Scalar subqueries in SELECT clause
- IN/NOT IN for set operations

### Data Integrity
- **PRIMARY KEY** constraints
- **FOREIGN KEY** relationships
- **CHECK** constraints for valid values
- **UNIQUE** constraints for preventing duplicates

---

## 📈 Performance Optimization

### Indexes Created
```sql
CREATE INDEX idx_transactions_account ON transactions(account_id);
CREATE INDEX idx_transactions_date ON transactions(transaction_date);
CREATE INDEX idx_gl_period ON general_ledger(period_year, period_month);
CREATE INDEX idx_bank_trans_id ON bank_statements(transaction_id);
```

### Query Optimization Techniques
1. Use appropriate indexes on frequently queried columns
2. Avoid SELECT * - specify needed columns
3. Use EXPLAIN QUERY PLAN to analyze performance
4. Batch INSERT operations for efficiency
5. Use appropriate data types (DECIMAL for financial data)

---

## 🔐 Security & Compliance

### Audit Trail Implementation
```sql
-- Trigger to log all changes
CREATE TRIGGER audit_transactions_insert
AFTER INSERT ON transactions
BEGIN
    INSERT INTO audit_log (event_type, table_name, record_id, description)
    VALUES ('INSERT', 'transactions', NEW.transaction_id, 
            'Transaction created: ' || NEW.description);
END;
```

### Data Validation
```sql
-- Ensure debits and credits balance
CREATE TRIGGER validate_transaction_balance
BEFORE INSERT ON transactions
BEGIN
    SELECT CASE
        WHEN (SELECT ABS(SUM(debit) - SUM(credit)) 
              FROM transactions 
              WHERE transaction_id = NEW.transaction_id) > 0.01
        THEN RAISE(ABORT, 'Transaction does not balance')
    END;
END;
```

---

## 💼 Business Impact

**SQL Skills Applied:**
- Complex multi-table joins for financial reporting
- Window functions for running balances
- Aggregations with GROUP BY for period analysis
- Subqueries for reconciliation logic
- Data validation and integrity constraints
- Audit trail implementation for compliance

**Real-World Applications:**
- Month-end close process automation
- Financial statement generation
- Bank reconciliation
- Data quality assurance
- Audit compliance
