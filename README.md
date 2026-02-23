# deltabook

Real-time orderbook analytics and data quality validation across crypto exchanges

## Getting Started

To get started with deltabook, follow these steps:

1. Clone the repository:
   ```bash
   git clone git@github.com:DjordjeVuckovic/deltabook.git
    ```
2. Execute docker compose:
   ```bash
   docker compose up -d
   ```
3. Run migrations:
   - I'm using [golang-migrate,](https://github.com/golang-migrate/migrate) but you can use any migration tool of your choice. Just make sure to point it to the correct database URL and migration files.
   ```bash
    migrate -path storage/migrations -database "postgres://root:root@localhost:54329/postgres?sslmode=disable" up
   ```

4. Run the application:
    Create [.env](.env.example) file in the root directory or export env variables directly in your terminal.
    ```bash
    uv run --env-file .env main.py
    ```