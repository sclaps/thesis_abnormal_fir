## 📚 Quick Reference

**Test in BigQuery Console first** - Validate queries before running in Python to see errors before running/executing

### Basic Query Pattern

First authenticate in terminal with
```bash
gcloud auth application-default login
```
and then with 
```bash
gcloud auth login
```

After that, get data through python with:

```python
from google.cloud import bigquery

client = bigquery.Client()
query = "SELECT * FROM `project.dataset.table` LIMIT 10"
df = client.query(query).to_dataframe()
```
