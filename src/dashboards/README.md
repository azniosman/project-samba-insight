# Samba Insight Dashboards

Interactive Streamlit dashboards for Brazilian E-Commerce Analytics.

## Dashboards

### 1. 🏠 Home
Welcome page with navigation and system status.

### 2. 📈 Executive Dashboard
High-level KPIs for executive decision-making:
- Total orders, customers, revenue
- Average order value and review score
- On-time delivery percentage
- Monthly revenue trends
- Order volume analysis
- Review score distribution
- Order status breakdown

### 3. 💰 Sales Operations
Detailed sales analysis:
- Top product categories by revenue and volume
- Geographic distribution by state
- Payment method analysis
- Top performing sellers
- Seller tier distribution

### 4. 👥 Customer Analytics
Customer behavioral analysis:
- Customer segmentation (loyal/repeat/one-time)
- Geographic customer distribution
- Review sentiment analysis
- Top customers by order volume
- Estimated lifetime value

### 5. ✅ Data Quality
Pipeline health monitoring:
- Overall data quality score
- Data completeness metrics
- Payment accuracy tracking
- Delivery performance analysis
- Order pipeline health
- dbt test results

## Running the Dashboard

### Prerequisites
```bash
# Ensure you're in the project root and virtual environment is activated
source .venv/bin/activate

# Install dependencies (if not already installed)
pip install -r requirements.txt
```

### Launch Dashboard
```bash
# Run from project root
streamlit run src/dashboards/app.py

# Or with custom port
streamlit run src/dashboards/app.py --server.port 8501
```

### Access
Open your browser to: **http://localhost:8501**

## Features

### Data Refresh
- Cached queries refresh every 5 minutes
- Manual refresh button on each dashboard
- Real-time data from BigQuery

### Interactive Elements
- Hover over charts for detailed information
- Filter data using sidebar controls
- Export data using Streamlit's built-in tools

### Performance
- Query results cached for 5 minutes
- BigQuery client connection reused
- Optimized queries for fast response

## Architecture

```
app.py                    # Main application entry point
├── db_connection.py      # BigQuery connection utilities
└── pages/
    ├── executive_dashboard.py
    ├── sales_operations.py
    ├── customer_analytics.py
    └── data_quality.py
```

## Data Sources

All dashboards query from BigQuery:
- **Project:** Configured via `GCP_PROJECT_ID` environment variable
- **Dataset:** Configured via `BQ_DATASET_WAREHOUSE` environment variable
  - Development: `dev_warehouse_warehouse`
  - Production: `warehouse`
- **Tables:**
  - fact_orders (99,441 rows)
  - dim_customer
  - dim_product
  - dim_seller
  - dim_date

The dataset and project are automatically resolved from your `.env` file configuration, ensuring the dashboard connects to the correct environment.

## Configuration

Dashboards use the same configuration as the rest of the project:
- Environment variables from `.env`
- BigQuery credentials from `GOOGLE_APPLICATION_CREDENTIALS`
- Project ID from `GCP_PROJECT_ID`

## Troubleshooting

### Connection Issues
- Verify `.env` file has correct credentials
- Test BigQuery connection: `dbt debug`
- Check service account permissions

### Slow Performance
- Queries are cached for 5 minutes
- BigQuery partitioning optimizes date-range queries
- Consider upgrading BigQuery slot reservations

### Display Issues
- Clear Streamlit cache: `streamlit cache clear`
- Restart the app
- Check browser console for errors

## Customization

### Adding New Dashboards
1. Create new file in `pages/` directory
2. Implement `render()` function
3. Add navigation option in `app.py`
4. Import the new page module

### Modifying Queries
- Update SQL queries in each dashboard file
- Queries are in the `run_query()` calls
- Test queries in BigQuery console first

### Styling
- Customize CSS in `app.py` markdown section
- Modify Plotly chart themes
- Adjust page layout (wide/centered)

## Deployment

### Local Development
```bash
streamlit run src/dashboards/app.py
```

### Production (Streamlit Cloud)
1. Push code to GitHub
2. Connect repository to Streamlit Cloud
3. Set environment variables in Streamlit Cloud settings
4. Deploy

### Docker
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["streamlit", "run", "src/dashboards/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

## Resources

- [Streamlit Documentation](https://docs.streamlit.io/)
- [Plotly Documentation](https://plotly.com/python/)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
