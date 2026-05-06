"""
Comprehensive Spark SQL MERGE (UPSERT) Examples
This script demonstrates various Spark SQL MERGE statements for updating or inserting records.
MERGE is also known as UPSERT (Update + Insert) operation.

Basic MERGE Syntax:
==================
MERGE INTO target_table AS target
USING source_table AS source
ON target.join_key = source.join_key
WHEN MATCHED THEN 
    UPDATE SET *
WHEN NOT MATCHED THEN 
    INSERT *

Example execution:
spark.sql(merge_statement)
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, when, lit, current_timestamp

def initialize_spark_session():
    """Initialize Spark session with Delta Lake support."""
    return SparkSession.builder \
        .appName("SparkSQLMergeExamples") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def basic_merge_example(spark):
    """
    Example 1: Basic MERGE statement - Update existing records, insert new ones
    This is the most common MERGE pattern.
    """
    
    # Create sample source data
    source_data = [
        (1, "John", "Doe", 25, "Engineering", 75000),
        (2, "Jane", "Smith", 28, "Marketing", 65000),
        (3, "Bob", "Johnson", 35, "Sales", 70000),
        (4, "Alice", "Brown", 30, "Engineering", 80000)  # New record
    ]
    
    source_df = spark.createDataFrame(source_data, 
                                     ["id", "first_name", "last_name", "age", "department", "salary"])
    
    # Create temporary view
    source_df.createOrReplaceTempView("source_employees")
    
    # Basic MERGE SQL statement
    merge_sql = """
    MERGE INTO delta.`/path/to/target/employees` AS target
    USING source_employees AS source
    ON target.id = source.id
    WHEN MATCHED THEN 
        UPDATE SET *
    WHEN NOT MATCHED THEN 
        INSERT *
    """
    
    print("Basic MERGE Statement:")
    print(merge_sql)
    
    # Execute the merge (uncomment when you have actual Delta table)
    # spark.sql(merge_sql)
    
    return merge_sql

def conditional_merge_example(spark):
    """
    Example 2: Conditional MERGE with WHERE clauses
    Only update records that meet certain conditions.
    """
    
    source_data = [
        (1, "John", "Doe", 26, "Engineering", 78000, "2024-01-15"),
        (2, "Jane", "Smith", 28, "Marketing", 67000, "2024-01-16"),
        (5, "Charlie", "Wilson", 29, "Finance", 72000, "2024-01-17")
    ]
    
    source_df = spark.createDataFrame(source_data, 
                                     ["id", "first_name", "last_name", "age", "department", "salary", "last_updated"])
    
    source_df.createOrReplaceTempView("source_updates")
    
    # Conditional MERGE - only update if salary increased
    conditional_merge_sql = """
    MERGE INTO delta.`/path/to/target/employees` AS target
    USING source_updates AS source
    ON target.id = source.id
    WHEN MATCHED AND source.salary > target.salary THEN 
        UPDATE SET 
            target.first_name = source.first_name,
            target.last_name = source.last_name,
            target.age = source.age,
            target.department = source.department,
            target.salary = source.salary,
            target.last_updated = source.last_updated
    WHEN NOT MATCHED THEN 
        INSERT (id, first_name, last_name, age, department, salary, last_updated)
        VALUES (source.id, source.first_name, source.last_name, source.age, 
                source.department, source.salary, source.last_updated)
    """
    
    print("Conditional MERGE Statement:")
    print(conditional_merge_sql)
    
    return conditional_merge_sql

def multi_column_join_merge_example(spark):
    """
    Example 3: MERGE with multiple join columns
    Useful for composite keys or when you need to match on multiple fields.
    """
    
    # Sales data with composite key (customer_id + product_id + date)
    sales_data = [
        (101, 'PROD-001', '2024-01-15', 2, 100.00, 200.00),
        (102, 'PROD-002', '2024-01-15', 1, 150.00, 150.00),
        (101, 'PROD-001', '2024-01-16', 3, 100.00, 300.00),  # Different date
        (103, 'PROD-003', '2024-01-15', 1, 200.00, 200.00)   # New record
    ]
    
    sales_df = spark.createDataFrame(sales_data, 
                                   ["customer_id", "product_id", "order_date", "quantity", "unit_price", "total_amount"])
    
    sales_df.createOrReplaceTempView("source_sales")
    
    # Multi-column join MERGE
    multi_join_merge_sql = """
    MERGE INTO delta.`/path/to/target/sales` AS target
    USING source_sales AS source
    ON target.customer_id = source.customer_id 
       AND target.product_id = source.product_id 
       AND target.order_date = source.order_date
    WHEN MATCHED THEN 
        UPDATE SET 
            target.quantity = target.quantity + source.quantity,
            target.total_amount = target.total_amount + source.total_amount
    WHEN NOT MATCHED THEN 
        INSERT *
    """
    
    print("Multi-Column Join MERGE Statement:")
    print(multi_join_merge_sql)
    
    return multi_join_merge_sql

def merge_with_delete_example(spark):
    """
    Example 4: MERGE with DELETE capability
    Delete records that meet certain conditions.
    """
    
    # Employee status updates
    status_data = [
        (1, "Active", "2024-01-15"),
        (2, "Terminated", "2024-01-10"),  # This should be deleted
        (3, "Active", "2024-01-12"),
        (4, "Inactive", "2024-01-14")
    ]
    
    status_df = spark.createDataFrame(status_data, ["id", "status", "status_date"])
    status_df.createOrReplaceTempView("employee_status_updates")
    
    # MERGE with DELETE
    merge_delete_sql = """
    MERGE INTO delta.`/path/to/target/employees` AS target
    USING employee_status_updates AS source
    ON target.id = source.id
    WHEN MATCHED AND source.status = 'Terminated' THEN 
        DELETE
    WHEN MATCHED THEN 
        UPDATE SET 
            target.status = source.status,
            target.status_date = source.status_date
    WHEN NOT MATCHED AND source.status != 'Terminated' THEN 
        INSERT (id, status, status_date)
        VALUES (source.id, source.status, source.status_date)
    """
    
    print("MERGE with DELETE Statement:")
    print(merge_delete_sql)
    
    return merge_delete_sql

def merge_with_expressions_example(spark):
    """
    Example 5: MERGE with complex expressions and transformations
    Use expressions, functions, and calculated fields in MERGE.
    """
    
    # Inventory updates with calculations
    inventory_data = [
        (1, "Widget A", 50, 25.00, "2024-01-15"),
        (2, "Widget B", -10, 30.00, "2024-01-15"),  # Negative quantity (return)
        (3, "Widget C", 75, 20.00, "2024-01-15")
    ]
    
    inventory_df = spark.createDataFrame(inventory_data, 
                                       ["product_id", "product_name", "quantity_change", "unit_cost", "update_date"])
    
    inventory_df.createOrReplaceTempView("inventory_updates")
    
    # MERGE with expressions and functions
    merge_expressions_sql = """
    MERGE INTO delta.`/path/to/target/inventory` AS target
    USING inventory_updates AS source
    ON target.product_id = source.product_id
    WHEN MATCHED THEN 
        UPDATE SET 
            target.quantity = target.quantity + source.quantity_change,
            target.unit_cost = source.unit_cost,
            target.total_value = (target.quantity + source.quantity_change) * source.unit_cost,
            target.last_updated = current_timestamp(),
            target.update_count = target.update_count + 1
    WHEN NOT MATCHED THEN 
        INSERT (product_id, product_name, quantity, unit_cost, total_value, last_updated, update_count)
        VALUES (source.product_id, source.product_name, source.quantity_change, 
                source.unit_cost, source.quantity_change * source.unit_cost, 
                current_timestamp(), 1)
    """
    
    print("MERGE with Expressions Statement:")
    print(merge_expressions_sql)
    
    return merge_expressions_sql

def merge_using_subquery_example(spark):
    """
    Example 6: MERGE using a subquery as source
    Use complex queries, aggregations, or joins as the source for MERGE.
    """
    
    # Create sample transaction data
    transaction_data = [
        (1, 101, 500.00, "2024-01-15", "Credit"),
        (2, 101, 200.00, "2024-01-15", "Debit"),
        (3, 102, 1000.00, "2024-01-15", "Credit"),
        (4, 102, 150.00, "2024-01-15", "Debit"),
        (5, 103, 750.00, "2024-01-15", "Credit")
    ]
    
    transaction_df = spark.createDataFrame(transaction_data, 
                                         ["transaction_id", "account_id", "amount", "transaction_date", "type"])
    
    transaction_df.createOrReplaceTempView("daily_transactions")
    
    # MERGE using aggregated subquery
    merge_subquery_sql = """
    MERGE INTO delta.`/path/to/target/account_balances` AS target
    USING (
        SELECT 
            account_id,
            SUM(CASE WHEN type = 'Credit' THEN amount ELSE -amount END) AS daily_net_amount,
            COUNT(*) AS transaction_count,
            MAX(transaction_date) AS last_transaction_date
        FROM daily_transactions
        WHERE transaction_date = '2024-01-15'
        GROUP BY account_id
    ) AS source
    ON target.account_id = source.account_id
    WHEN MATCHED THEN 
        UPDATE SET 
            target.balance = target.balance + source.daily_net_amount,
            target.last_updated = current_timestamp(),
            target.transaction_count = target.transaction_count + source.transaction_count
    WHEN NOT MATCHED THEN 
        INSERT (account_id, balance, last_updated, transaction_count)
        VALUES (source.account_id, source.daily_net_amount, 
                current_timestamp(), source.transaction_count)
    """
    
    print("MERGE with Subquery Statement:")
    print(merge_subquery_sql)
    
    return merge_subquery_sql

def delta_api_merge_example(spark):
    """
    Example 7: Using Delta Table API for MERGE operations
    Alternative to SQL MERGE using Delta Lake's Python API.
    """
    
    # Sample data
    source_data = [
        (1, "Product A", 100, 25.00),
        (2, "Product B", 150, 30.00),
        (3, "Product C", 75, 20.00)
    ]
    
    source_df = spark.createDataFrame(source_data, ["id", "name", "quantity", "price"])
    
    # Using Delta Table API (when you have actual Delta table)
    """
    delta_table = DeltaTable.forPath(spark, "/path/to/target/products")
    
    delta_table.alias("target").merge(
        source_df.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(set={
        "target.name": "source.name",
        "target.quantity": "target.quantity + source.quantity",
        "target.price": "source.price"
    }).whenNotMatchedInsert(values={
        "id": "source.id",
        "name": "source.name", 
        "quantity": "source.quantity",
        "price": "source.price"
    }).execute()
    """
    
    print("Delta Table API MERGE Example:")
    print("delta_table.alias('target').merge(...).whenMatchedUpdate(...).whenNotMatchedInsert(...).execute()")
    
    # Equivalent SQL for the above Delta API call
    equivalent_sql = """
    MERGE INTO delta.`/path/to/target/products` AS target
    USING source_products AS source
    ON target.id = source.id
    WHEN MATCHED THEN 
        UPDATE SET 
            target.name = source.name,
            target.quantity = target.quantity + source.quantity,
            target.price = source.price
    WHEN NOT MATCHED THEN 
        INSERT (id, name, quantity, price)
        VALUES (source.id, source.name, source.quantity, source.price)
    """
    
    print("Equivalent SQL:")
    print(equivalent_sql)
    
    return equivalent_sql

# Main execution function
if __name__ == "__main__":
    spark = initialize_spark_session()
    
    print("="*60)
    print("SPARK SQL MERGE (UPSERT) EXAMPLES")
    print("="*60)
    
    try:
        # Run all examples
        print("\n" + "="*40)
        basic_merge_example(spark)
        
        print("\n" + "="*40)
        conditional_merge_example(spark)
        
        print("\n" + "="*40)
        multi_column_join_merge_example(spark)
        
        print("\n" + "="*40)
        merge_with_delete_example(spark)
        
        print("\n" + "="*40)
        merge_with_expressions_example(spark)
        
        print("\n" + "="*40)
        merge_using_subquery_example(spark)
        
        print("\n" + "="*40)
        delta_api_merge_example(spark)
        
    finally:
        # Clean up
        spark.stop()

"""
QUICK REFERENCE - SPARK SQL MERGE SYNTAX:

Basic MERGE Template:
MERGE INTO target_table AS target
USING source_table AS source
ON join_condition
WHEN MATCHED THEN 
    UPDATE SET column1 = value1, column2 = value2, ...
WHEN NOT MATCHED THEN 
    INSERT (column1, column2, ...) VALUES (value1, value2, ...)
WHEN NOT MATCHED BY SOURCE THEN 
    DELETE

Key Points:
1. MERGE is atomic - either all operations succeed or none do
2. Can use UPDATE SET * to update all columns with matching names
3. Can use INSERT * to insert all columns
4. Conditional clauses can include WHERE conditions
5. Can have multiple WHEN clauses with different conditions
6. Use current_timestamp(), current_date() for audit columns
7. Supports complex expressions and functions in SET clauses
8. Works with both SQL API and Delta Table API
"""