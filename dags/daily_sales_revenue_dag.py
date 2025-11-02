from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt

PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'kiwilytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Task 1: fetch order_data including prices from products
def fetch_order_data():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()

    query = """
    SELECT
        o.orderdate::date AS sale_date,
        od.productid,
        p.price,
        od.quantity,
        od.orderid
    FROM orders o 
    JOIN order_details od ON o.orderid = od.orderid
    JOIN products p ON od.productid = p.productid
    """
    
    df = pd.read_sql(query, conn)
    df.to_csv('/home/kiwilytics/output/daily_sales_data.csv', index=False)
    print("Order data fetched and saved successfully")
    conn.close()

# Task 2: process total daily revenue and find specific date revenue
def process_daily_revenue():
    df = pd.read_csv('/home/kiwilytics/output/daily_sales_data.csv')
    df['total_revenue'] = df['quantity'] * df['price']
    
    revenue_per_day = df.groupby('sale_date')['total_revenue'].sum().reset_index()
    revenue_per_day.to_csv('/home/kiwilytics/output/daily_revenue.csv', index=False)
    
    # Find revenue for 1996-08-08
    target_date = '1996-08-08'
    target_revenue = revenue_per_day[revenue_per_day['sale_date'] == target_date]
    
    if len(target_revenue) > 0:
        revenue_amount = target_revenue['total_revenue'].values[0]
        print(f"TOTAL REVENUE ON {target_date}: ${revenue_amount:.2f}")
        
        # Save the specific revenue result
        with open('/home/kiwilytics/output/revenue_1996_08_08.txt', 'w') as f:
            f.write(str(revenue_amount))
    else:
        print(f"No sales data found for {target_date}")
        with open('/home/kiwilytics/output/revenue_1996_08_08.txt', 'w') as f:
            f.write('0')
    
    print("Daily revenue processed and saved")

# Task 3: plot_daily_revenue
def plot_daily_revenue():
    df = pd.read_csv('/home/kiwilytics/output/daily_revenue.csv')
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    
    plt.figure(figsize=(12, 6))
    plt.plot(df['sale_date'], df['total_revenue'], marker='o', linestyle='-', linewidth=1, markersize=3)
    plt.title('Daily Sales Revenue Over Time')
    plt.xlabel('Date')
    plt.ylabel('Daily Revenue ($)')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    output_path = '/home/kiwilytics/output/daily_revenue_plot.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f'Revenue chart saved to {output_path}')
    
    # Also display the 1996-08-08 revenue on the chart for reference
    target_date = pd.to_datetime('1996-08-08')
    target_revenue = df[df['sale_date'] == target_date]
    if len(target_revenue) > 0:
        revenue = target_revenue['total_revenue'].values[0]
        plt.annotate(f'1996-08-08: ${revenue:.2f}', 
                    xy=(target_date, revenue), 
                    xytext=(target_date + pd.Timedelta(days=30), revenue),
                    arrowprops=dict(arrowstyle='->', color='red'))
        plt.savefig('/home/kiwilytics/output/daily_revenue_plot_annotated.png', dpi=300, bbox_inches='tight')

# Task 4: Display final answer
def display_final_answer():
    """Display the final answer for the assignment question"""
    try:
        with open('/home/kiwilytics/output/revenue_1996_08_08.txt', 'r') as f:
            revenue = float(f.read())
        
        print("=" * 50)
        print(f"ASSIGNMENT ANSWER:")
        print(f"What is the total revenue on 1996-08-08?")
        print(f"ANSWER: ${revenue:.2f}")
        print("=" * 50)
        
        return revenue
    except FileNotFoundError:
        print("Revenue file not found. Run process_daily_revenue task first.")
        return 0

# Define the DAG
with DAG(
    dag_id="daily_sales_revenue_analysis",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Extract, calculate and visualize daily sales revenue from PostgreSQL database',
) as dag:

    task_fetch_data = PythonOperator(
        task_id="fetch_order_data",
        python_callable=fetch_order_data,
    )
    
    task_process_revenue = PythonOperator(
        task_id="process_daily_revenue",
        python_callable=process_daily_revenue,
    )
    
    task_plot_revenue = PythonOperator(
        task_id="plot_daily_revenue",
        python_callable=plot_daily_revenue,
    )
    
    task_display_answer = PythonOperator(
        task_id="display_final_answer",
        python_callable=display_final_answer,
    )
    
    task_fetch_data >> task_process_revenue >> [task_plot_revenue, task_display_answer]
