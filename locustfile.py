from locust import HttpUser, task, between, constant
import random

BEARER_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjEiLCJubSI6IkFkbWluIEZsZXh1cmlvIiwiZXhwIjoxNzU5ODkxNDE2LCJhdCI6MTc1OTgwNTAxNiwicmwiOiJiYW5rX3R5cGVzLzEyNyxiYW5rcy8xMjcsY3VzdG9tZXJzLzEyNyxmbHhfcm9sZXMvMTI3LGZseF91c2Vycy8xMjcscHJvZHVjdHMvMTI3LHNhbGVzLzEyNyxzYWxlc19pdGVtcy8xMjciLCJjcyI6IiJ9.j7_9HTZkcVlkoNXyrghKyt0A4IoEbADvVNeD_mzHtDI"

class APIUser(HttpUser):
    # Semua user pasti nunggu 10 detik antar request
    wait_time = between(1, 3)

    def on_start(self):
        self.headers = {
            "Authorization": f"Bearer {BEARER_TOKEN}",
            "Accept": "application/json",
        }

    @task(7)
    def get_banks(self):
        self.client.get("/banks", headers=self.headers)

    @task(3)
    def create_book(self):
        book_id = random.randint(1000, 9999)
        files = {
            "name": (None, f"Bank {book_id}"),
            "bank_type_id": (None, "BMG/2025/10/001"),
        }
        with self.client.post(
            "/banks",
            headers=self.headers,
            files=files,
            catch_response=True
        ) as response:
            if response.status_code not in (200, 201):
                print("POST ERROR" + response.text)
                response.failure(f"POST /banks gagal: {response.status_code} {response.text}")