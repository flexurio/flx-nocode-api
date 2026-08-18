
<img width="1252" height="658" alt="Image" src="https://github.com/user-attachments/assets/044ad54f-531b-4b35-a0be-8622d82f2c7a" />
<img width="1135" height="451" alt="Image" src="https://github.com/user-attachments/assets/a16a1ffe-1f95-4dd3-9693-1772dc82b391" />
<img width="1442" height="341" alt="Image" src="https://github.com/user-attachments/assets/b3070185-4a60-448c-a442-cb44c95bbdb6" />

# [BUG] Invalid Foreign Key Value Error on POST Requests Using Form-Data

## Description
When submitting a POST request to create an entity that contains a foreign key relation using `multipart/form-data`, the API returns a `400 Bad Request` claiming the foreign key is invalid, even when the referenced ID exists in the database.

## Environment & Project Details
- **Project Link (Google Drive)**: [flx-nocode-api-demo](https://drive.google.com/file/d/16Y76U7JZIx60lIxtO73SnFHaPMjg0El2/view?usp=sharing)
- **Login Credentials**:
  - **Username**: `admin`
  - **Password**: `3599`

## Steps to Reproduce
1. Ensure the parent table (e.g., `master_inventory_category`) has an existing record with an ID (e.g., `id = 1`).
2. Make a POST request to insert a record into the child table (e.g., `master_inventory_item`) referencing that ID via `multipart/form-data`.
3. Example `curl` command:
```bash
curl --location 'http://0.0.0.0:8080/master_inventory_item' \
--header 'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjEiLCJubSI6IkFkbWluIEZsZXh1cmlvIiwiZXhwIjoxNzg2NTA4NzU4LCJhdCI6MTc4NjQyMjM1OCwicmwiOiIxMjcsMTI3IiwiY3MiOiIifQ.ugkR8WVxDBMoo_QYQfzAvAAfvwmnXufsm_cY4mHSd0w' \
--form 'sku="1"' \
--form 'name="Kabel"' \
--form 'category_id="1"' \
--form 'unit="Pcs"' \
--form 'min_stock="1"' \
--form 'current_stock="1"'
```

## Expected Behavior
The entry should be inserted successfully.

## Actual Behavior
The API responds with:
```json
{
  "success": false,
  "message": "Invalid foreign key value for column 'category_id' referencing table 'master_inventory_category'",
  "total_data": 0,
  "data": null
}
```

## Possible Cause
Since the request payload is sent as `multipart/form-data`, integer fields (such as `category_id`) are received as strings (e.g. `"1"`). If the API validation or database binding layer strictly checks the data type or fails to coerce the string representation into an integer prior to checking foreign key constraints, it will fail to match the existing ID in the parent table.

