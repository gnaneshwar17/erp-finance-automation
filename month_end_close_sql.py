"""
SQL Database Integration for Month-End Close Automation
Implements SQLite database for transaction storage, querying, and audit trail
"""

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

class MonthEndCloseDatabase:
    """
    Database layer for month-end close automation with SQL operations
    """
    
    def __init__(self, db_path='/home/claude/month_end_close.db'):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection"""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        print(f"✓ Connected to database: {self.db_path}")
        
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("✓ Database connection closed")
    
    def create_schema(self):
        """Create complete database schema for accounting system"""
        
        # Chart of Accounts table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS chart_of_accounts (
                account_id TEXT PRIMARY KEY,
                account_name TEXT NOT NULL,
                account_type TEXT NOT NULL CHECK(account_type IN ('Asset', 'Liability', 'Equity', 'Revenue', 'Expense')),
                parent_account TEXT,
                is_active INTEGER DEFAULT 1,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Transactions table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT NOT NULL,
                line_number INTEGER NOT NULL,
                transaction_date DATE NOT NULL,
                account_id TEXT NOT NULL,
                debit DECIMAL(15,2) DEFAULT 0,
                credit DECIMAL(15,2) DEFAULT 0,
                description TEXT,
                reference_number TEXT,
                posted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                posted_by TEXT DEFAULT 'SYSTEM',
                PRIMARY KEY (transaction_id, line_number),
                FOREIGN KEY (account_id) REFERENCES chart_of_accounts(account_id)
            )
        """)
        
        # General Ledger table (aggregated view)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS general_ledger (
                gl_entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                period_year INTEGER NOT NULL,
                period_month INTEGER NOT NULL,
                beginning_balance DECIMAL(15,2) DEFAULT 0,
                total_debits DECIMAL(15,2) DEFAULT 0,
                total_credits DECIMAL(15,2) DEFAULT 0,
                ending_balance DECIMAL(15,2) DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (account_id) REFERENCES chart_of_accounts(account_id),
                UNIQUE(account_id, period_year, period_month)
            )
        """)
        
        # Bank Statements table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS bank_statements (
                statement_id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_date DATE NOT NULL,
                description TEXT,
                amount DECIMAL(15,2) NOT NULL,
                transaction_id TEXT,
                cleared_flag INTEGER DEFAULT 0,
                reconciled_flag INTEGER DEFAULT 0,
                reconciliation_date TIMESTAMP,
                statement_reference TEXT
            )
        """)
        
        # Reconciliation table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS reconciliations (
                reconciliation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                reconciliation_date DATE NOT NULL,
                period_year INTEGER NOT NULL,
                period_month INTEGER NOT NULL,
                book_balance DECIMAL(15,2),
                bank_balance DECIMAL(15,2),
                outstanding_items_count INTEGER,
                outstanding_items_amount DECIMAL(15,2),
                bank_only_items_count INTEGER,
                bank_only_items_amount DECIMAL(15,2),
                variance DECIMAL(15,2),
                reconciled_flag INTEGER,
                completed_by TEXT,
                completed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Audit Log table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                event_type TEXT NOT NULL,
                table_name TEXT,
                record_id TEXT,
                old_value TEXT,
                new_value TEXT,
                user_name TEXT,
                description TEXT
            )
        """)
        
        self.conn.commit()
        print("✓ Database schema created successfully")
        
    def insert_chart_of_accounts(self):
        """Insert standard chart of accounts"""
        accounts = [
            ('1000', 'Cash', 'Asset', None),
            ('1010', 'Petty Cash', 'Asset', '1000'),
            ('1100', 'Accounts Receivable', 'Asset', None),
            ('1200', 'Inventory', 'Asset', None),
            ('1500', 'Fixed Assets', 'Asset', None),
            ('2000', 'Accounts Payable', 'Liability', None),
            ('2100', 'Accrued Liabilities', 'Liability', None),
            ('2500', 'Long-term Debt', 'Liability', None),
            ('3000', 'Common Stock', 'Equity', None),
            ('3100', 'Retained Earnings', 'Equity', None),
            ('4000', 'Revenue', 'Revenue', None),
            ('4100', 'Service Revenue', 'Revenue', '4000'),
            ('5000', 'Cost of Goods Sold', 'Expense', None),
            ('6000', 'Operating Expenses', 'Expense', None),
            ('6100', 'Salaries Expense', 'Expense', '6000'),
            ('6200', 'Rent Expense', 'Expense', '6000'),
        ]
        
        self.cursor.executemany("""
            INSERT OR IGNORE INTO chart_of_accounts (account_id, account_name, account_type, parent_account)
            VALUES (?, ?, ?, ?)
        """, accounts)
        
        self.conn.commit()
        print(f"✓ Inserted {len(accounts)} accounts into chart of accounts")
        
    def insert_transactions(self, transactions_df):
        """Insert transactions from DataFrame into database"""
        
        # Add line numbers for multi-line transactions
        transactions_data = []
        line_counter = {}
        
        for _, row in transactions_df.iterrows():
            # Extract components
            trans_id = row['transaction_id']
            trans_date = row['date'].strftime('%Y-%m-%d')
            account = row['account']
            debit = float(row['debit'])
            credit = float(row['credit'])
            description = row['description']
            
            # Track line numbers per transaction
            if trans_id not in line_counter:
                line_counter[trans_id] = 1
            else:
                line_counter[trans_id] += 1
            
            line_number = line_counter[trans_id]
            
            transactions_data.append((
                trans_id, line_number, trans_date, account,
                debit, credit, description, trans_id
            ))
        
        self.cursor.executemany("""
            INSERT INTO transactions 
            (transaction_id, line_number, transaction_date, account_id, 
             debit, credit, description, reference_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, transactions_data)
        
        self.conn.commit()
        print(f"✓ Inserted {len(transactions_data)} transaction lines into database")
        
        # Log audit trail
        self.log_audit_event('INSERT', 'transactions', None, 
                            f'Batch insert of {len(transactions_data)} transactions')
        
    def insert_bank_statement(self, bank_statement_df):
        """Insert bank statement data into database"""
        bank_data = []
        
        for _, row in bank_statement_df.iterrows():
            bank_data.append((
                row['date'].strftime('%Y-%m-%d'),
                row['description'],
                float(row['amount']),
                row['transaction_id'],
                1 if row.get('cleared', True) else 0
            ))
        
        self.cursor.executemany("""
            INSERT INTO bank_statements 
            (transaction_date, description, amount, transaction_id, cleared_flag)
            VALUES (?, ?, ?, ?, ?)
        """, bank_data)
        
        self.conn.commit()
        print(f"✓ Inserted {len(bank_data)} bank statement entries")
        
    def post_to_general_ledger(self, year, month):
        """Aggregate transactions and post to general ledger"""
        
        # Calculate totals for each account
        self.cursor.execute("""
            INSERT OR REPLACE INTO general_ledger 
            (account_id, period_year, period_month, total_debits, total_credits, ending_balance)
            SELECT 
                t.account_id,
                ? as period_year,
                ? as period_month,
                SUM(t.debit) as total_debits,
                SUM(t.credit) as total_credits,
                SUM(t.debit) - SUM(t.credit) as ending_balance
            FROM transactions t
            WHERE strftime('%Y', t.transaction_date) = ?
            AND strftime('%m', t.transaction_date) = ?
            GROUP BY t.account_id
        """, (year, month, str(year), f"{month:02d}"))
        
        self.conn.commit()
        print(f"✓ Posted transactions to General Ledger for {year}-{month:02d}")
        
        # Log audit trail
        self.log_audit_event('POST', 'general_ledger', None,
                            f'Month-end posting for {year}-{month:02d}')
    
    def get_trial_balance(self, year=None, month=None):
        """Generate trial balance report using SQL"""
        
        if year and month:
            query = """
                SELECT 
                    c.account_id,
                    c.account_name,
                    c.account_type,
                    COALESCE(g.total_debits, 0) as total_debits,
                    COALESCE(g.total_credits, 0) as total_credits,
                    COALESCE(g.ending_balance, 0) as ending_balance,
                    CASE 
                        WHEN COALESCE(g.ending_balance, 0) > 0 THEN COALESCE(g.ending_balance, 0)
                        ELSE 0 
                    END as debit_balance,
                    CASE 
                        WHEN COALESCE(g.ending_balance, 0) < 0 THEN -COALESCE(g.ending_balance, 0)
                        ELSE 0 
                    END as credit_balance
                FROM chart_of_accounts c
                LEFT JOIN general_ledger g ON c.account_id = g.account_id
                    AND g.period_year = ?
                    AND g.period_month = ?
                WHERE c.is_active = 1
                ORDER BY c.account_id
            """
            df = pd.read_sql_query(query, self.conn, params=(year, month))
        else:
            query = """
                SELECT 
                    t.account_id,
                    c.account_name,
                    c.account_type,
                    SUM(t.debit) as total_debits,
                    SUM(t.credit) as total_credits,
                    SUM(t.debit) - SUM(t.credit) as ending_balance,
                    CASE 
                        WHEN SUM(t.debit) - SUM(t.credit) > 0 
                        THEN SUM(t.debit) - SUM(t.credit)
                        ELSE 0 
                    END as debit_balance,
                    CASE 
                        WHEN SUM(t.debit) - SUM(t.credit) < 0 
                        THEN -(SUM(t.debit) - SUM(t.credit))
                        ELSE 0 
                    END as credit_balance
                FROM transactions t
                JOIN chart_of_accounts c ON t.account_id = c.account_id
                GROUP BY t.account_id, c.account_name, c.account_type
                ORDER BY t.account_id
            """
            df = pd.read_sql_query(query, self.conn)
        
        return df
    
    def get_account_activity(self, account_id, start_date=None, end_date=None):
        """Query detailed activity for specific account"""
        
        query = """
            SELECT 
                t.transaction_id,
                t.transaction_date,
                t.description,
                t.debit,
                t.credit,
                t.debit - t.credit as net_amount,
                t.reference_number
            FROM transactions t
            WHERE t.account_id = ?
        """
        
        params = [account_id]
        
        if start_date:
            query += " AND t.transaction_date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND t.transaction_date <= ?"
            params.append(end_date)
        
        query += " ORDER BY t.transaction_date, t.transaction_id"
        
        df = pd.read_sql_query(query, self.conn, params=params)
        return df
    
    def perform_bank_reconciliation(self, year, month):
        """SQL-based bank reconciliation"""
        
        # Get book balance from GL
        self.cursor.execute("""
            SELECT ending_balance
            FROM general_ledger
            WHERE account_id = '1000'
            AND period_year = ?
            AND period_month = ?
        """, (year, month))
        
        result = self.cursor.fetchone()
        book_balance = result[0] if result else 0
        
        # Get bank balance
        self.cursor.execute("""
            SELECT SUM(amount)
            FROM bank_statements
            WHERE strftime('%Y', transaction_date) = ?
            AND strftime('%m', transaction_date) = ?
        """, (str(year), f"{month:02d}"))
        
        result = self.cursor.fetchone()
        bank_balance = result[0] if result and result[0] else 0
        
        # Outstanding items (in books but not in bank)
        self.cursor.execute("""
            SELECT COUNT(*), COALESCE(SUM(t.debit - t.credit), 0)
            FROM transactions t
            WHERE t.account_id = '1000'
            AND NOT EXISTS (
                SELECT 1 FROM bank_statements b
                WHERE b.transaction_id = t.transaction_id
            )
        """)
        
        outstanding = self.cursor.fetchone()
        outstanding_count = outstanding[0]
        outstanding_amount = outstanding[1]
        
        # Bank-only items
        self.cursor.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount), 0)
            FROM bank_statements
            WHERE transaction_id NOT IN (
                SELECT transaction_id FROM transactions
            )
        """)
        
        bank_only = self.cursor.fetchone()
        bank_only_count = bank_only[0]
        bank_only_amount = bank_only[1]
        
        # Calculate adjusted balances
        adjusted_book = book_balance + bank_only_amount
        adjusted_bank = bank_balance - outstanding_amount
        variance = adjusted_book - adjusted_bank
        reconciled = abs(variance) < 0.01
        
        # Insert reconciliation record
        self.cursor.execute("""
            INSERT INTO reconciliations 
            (reconciliation_date, period_year, period_month, book_balance, bank_balance,
             outstanding_items_count, outstanding_items_amount, bank_only_items_count,
             bank_only_items_amount, variance, reconciled_flag, completed_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now().date(), year, month, book_balance, bank_balance,
              outstanding_count, outstanding_amount, bank_only_count, bank_only_amount,
              variance, 1 if reconciled else 0, 'SYSTEM'))
        
        self.conn.commit()
        
        return {
            'book_balance': book_balance,
            'bank_balance': bank_balance,
            'outstanding_items_count': outstanding_count,
            'outstanding_items_amount': outstanding_amount,
            'bank_only_items_count': bank_only_count,
            'bank_only_items_amount': bank_only_amount,
            'adjusted_book_balance': adjusted_book,
            'adjusted_bank_balance': adjusted_bank,
            'variance': variance,
            'reconciled': reconciled
        }
    
    def get_financial_statements(self):
        """Generate financial statements using SQL aggregations"""
        
        # Income Statement
        self.cursor.execute("""
            SELECT 
                SUM(CASE WHEN c.account_type = 'Revenue' THEN t.credit ELSE 0 END) as revenue,
                SUM(CASE WHEN c.account_type = 'Expense' THEN t.debit ELSE 0 END) as expenses
            FROM transactions t
            JOIN chart_of_accounts c ON t.account_id = c.account_id
        """)
        
        income_data = self.cursor.fetchone()
        revenue = income_data[0] if income_data[0] else 0
        expenses = income_data[1] if income_data[1] else 0
        net_income = revenue - expenses
        
        # Balance Sheet
        self.cursor.execute("""
            SELECT 
                SUM(CASE WHEN c.account_type = 'Asset' THEN t.debit - t.credit ELSE 0 END) as assets,
                SUM(CASE WHEN c.account_type = 'Liability' THEN t.credit - t.debit ELSE 0 END) as liabilities,
                SUM(CASE WHEN c.account_type = 'Equity' THEN t.credit - t.debit ELSE 0 END) as equity
            FROM transactions t
            JOIN chart_of_accounts c ON t.account_id = c.account_id
        """)
        
        balance_data = self.cursor.fetchone()
        assets = balance_data[0] if balance_data[0] else 0
        liabilities = balance_data[1] if balance_data[1] else 0
        equity = balance_data[2] if balance_data[2] else 0
        
        total_equity = equity + net_income
        balanced = abs(assets - (liabilities + total_equity)) < 0.01
        
        return {
            'income_statement': {
                'revenue': revenue,
                'expenses': expenses,
                'net_income': net_income
            },
            'balance_sheet': {
                'assets': assets,
                'liabilities': liabilities,
                'equity': total_equity,
                'balanced': balanced
            }
        }
    
    def log_audit_event(self, event_type, table_name, record_id, description):
        """Log audit trail for compliance"""
        self.cursor.execute("""
            INSERT INTO audit_log 
            (event_type, table_name, record_id, description, user_name)
            VALUES (?, ?, ?, ?, ?)
        """, (event_type, table_name, record_id, description, 'SYSTEM'))
        
        self.conn.commit()
    
    def get_audit_trail(self, limit=50):
        """Retrieve recent audit trail"""
        query = """
            SELECT 
                audit_id,
                event_timestamp,
                event_type,
                table_name,
                description
            FROM audit_log
            ORDER BY event_timestamp DESC
            LIMIT ?
        """
        
        df = pd.read_sql_query(query, self.conn, params=(limit,))
        return df
    
    def run_data_quality_checks(self):
        """SQL-based data quality validation queries"""
        
        checks = {}
        
        # Check 1: Verify debits = credits for all transactions
        self.cursor.execute("""
            SELECT 
                transaction_id,
                SUM(debit) as total_debits,
                SUM(credit) as total_credits,
                ABS(SUM(debit) - SUM(credit)) as variance
            FROM transactions
            GROUP BY transaction_id
            HAVING ABS(SUM(debit) - SUM(credit)) > 0.01
        """)
        
        unbalanced = self.cursor.fetchall()
        checks['unbalanced_transactions'] = len(unbalanced)
        
        # Check 2: Verify GL totals match transaction totals
        self.cursor.execute("""
            SELECT 
                (SELECT SUM(debit) FROM transactions) as trans_debits,
                (SELECT SUM(credit) FROM transactions) as trans_credits,
                (SELECT SUM(total_debits) FROM general_ledger) as gl_debits,
                (SELECT SUM(total_credits) FROM general_ledger) as gl_credits
        """)
        
        totals = self.cursor.fetchone()
        checks['transaction_gl_match'] = (
            abs((totals[0] or 0) - (totals[2] or 0)) < 0.01 and
            abs((totals[1] or 0) - (totals[3] or 0)) < 0.01
        )
        
        # Check 3: Orphaned transactions (invalid account IDs)
        self.cursor.execute("""
            SELECT COUNT(*)
            FROM transactions t
            WHERE NOT EXISTS (
                SELECT 1 FROM chart_of_accounts c
                WHERE c.account_id = t.account_id
            )
        """)
        
        orphaned = self.cursor.fetchone()[0]
        checks['orphaned_transactions'] = orphaned
        
        # Check 4: Duplicate transaction IDs
        self.cursor.execute("""
            SELECT transaction_id, COUNT(*) as count
            FROM (
                SELECT DISTINCT transaction_id, line_number
                FROM transactions
            )
            GROUP BY transaction_id
            HAVING COUNT(*) != 2  -- Assuming each transaction has 2 lines (debit & credit)
        """)
        
        duplicates = self.cursor.fetchall()
        checks['unusual_transaction_lines'] = len(duplicates)
        
        return checks


def main_with_sql():
    """Main execution with SQL integration"""
    from month_end_close_automation import MonthEndCloseAutomation
    
    print("="*70)
    print("MONTH-END CLOSE AUTOMATION WITH SQL DATABASE")
    print("="*70)
    
    # Initialize database
    db = MonthEndCloseDatabase()
    db.connect()
    
    # Create schema
    print("\n[DATABASE SETUP]")
    db.create_schema()
    db.insert_chart_of_accounts()
    
    # Generate transactions using original system
    print("\n[TRANSACTION GENERATION]")
    automation = MonthEndCloseAutomation()
    automation.generate_sample_transactions(100)
    
    # Insert into database
    print("\n[DATABASE OPERATIONS]")
    db.insert_transactions(automation.transactions)
    
    # Post to GL
    current_year = automation.close_date.year
    current_month = automation.close_date.month
    db.post_to_general_ledger(current_year, current_month)
    
    # Generate and insert bank statement
    automation.generate_bank_statement()
    db.insert_bank_statement(automation.bank_statement)
    
    # SQL-based trial balance
    print("\n[SQL QUERY: TRIAL BALANCE]")
    trial_balance = db.get_trial_balance(current_year, current_month)
    print(trial_balance.to_string(index=False))
    
    # SQL-based reconciliation
    print("\n[SQL QUERY: BANK RECONCILIATION]")
    recon = db.perform_bank_reconciliation(current_year, current_month)
    print(f"✓ Book Balance: ${recon['book_balance']:,.2f}")
    print(f"✓ Bank Balance: ${recon['bank_balance']:,.2f}")
    print(f"✓ Outstanding Items: {recon['outstanding_items_count']} (${recon['outstanding_items_amount']:,.2f})")
    print(f"✓ Reconciled: {recon['reconciled']}")
    
    # SQL-based financial statements
    print("\n[SQL QUERY: FINANCIAL STATEMENTS]")
    statements = db.get_financial_statements()
    print(f"\nIncome Statement:")
    print(f"  Revenue: ${statements['income_statement']['revenue']:,.2f}")
    print(f"  Expenses: ${statements['income_statement']['expenses']:,.2f}")
    print(f"  Net Income: ${statements['income_statement']['net_income']:,.2f}")
    
    print(f"\nBalance Sheet:")
    print(f"  Assets: ${statements['balance_sheet']['assets']:,.2f}")
    print(f"  Liabilities: ${statements['balance_sheet']['liabilities']:,.2f}")
    print(f"  Equity: ${statements['balance_sheet']['equity']:,.2f}")
    print(f"  Balanced: {statements['balance_sheet']['balanced']}")
    
    # Data quality checks
    print("\n[SQL QUERY: DATA QUALITY CHECKS]")
    quality_checks = db.run_data_quality_checks()
    print(f"✓ Unbalanced Transactions: {quality_checks['unbalanced_transactions']}")
    print(f"✓ Transaction/GL Match: {quality_checks['transaction_gl_match']}")
    print(f"✓ Orphaned Transactions: {quality_checks['orphaned_transactions']}")
    print(f"✓ Unusual Transaction Lines: {quality_checks['unusual_transaction_lines']}")
    
    # Audit trail
    print("\n[SQL QUERY: AUDIT TRAIL]")
    audit = db.get_audit_trail(10)
    print(audit.to_string(index=False))
    
    # Account activity example
    print("\n[SQL QUERY: CASH ACCOUNT ACTIVITY]")
    cash_activity = db.get_account_activity('1000')
    print(f"Total Cash Transactions: {len(cash_activity)}")
    if len(cash_activity) > 0:
        print(cash_activity.head(10).to_string(index=False))
    
    print("\n" + "="*70)
    print("SQL DATABASE INTEGRATION COMPLETED")
    print(f"Database location: {db.db_path}")
    print("="*70)
    
    # Close connection
    db.close()
    
    return db, automation, trial_balance, recon, statements


if __name__ == "__main__":
    db, automation, trial_balance, recon, statements = main_with_sql()
