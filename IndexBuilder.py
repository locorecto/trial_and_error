from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker

class IndexRebuilder:
    def __init__(self, server, database, username, password):
        # Connection string for SQL Server
        self.connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        
    def verify_all_results_exist(self, session, given_id):
        try:
            # Read and check records from the RunData table
            query = text("SELECT * FROM RunData WHERE id = :given_id AND value BETWEEN 10 AND 20")
            result = session.execute(query, {"given_id": given_id})
            records = result.fetchall()

            if not records:
                print(f"No records found in RunData for id {given_id} within the specified range.")
                return False
            else:
                print(f"Records found in RunData for id {given_id} within the specified range:")
                for record in records:
                    print(record)
                return True

        except exc.SQLAlchemyError as e:
            # Capture any exception
            print(f"An error occurred during verification: {e}")
            return False

    def execute_index_builder(self, session):
        try:
            # Call stored procedure
            session.execute(text("EXEC dbo.YourStoredProcedure"))

            # Commit the changes
            session.commit()

        except exc.SQLAlchemyError as e:
            # Capture any exception
            print(f"An error occurred during Index Builder execution: {e}")

    def run(self, given_id):
        try:
            # Creating the engine and session
            engine = create_engine(self.connection_string, echo=True)
            Session = sessionmaker(bind=engine)
            session = Session()

            # Step 1: Verify all results exist in the RunData table
            results_exist = self.verify_all_results_exist(session, given_id)

            # Step 2: Execute the Index Builder if results exist
            if results_exist:
                self.execute_index_builder(session)

        except exc.SQLAlchemyError as e:
            # Capture any exception
            print(f"An error occurred: {e}")

        finally:
            # Close the session
            session.close()

# Example usage
if __name__ == "__main__":
    # Replace the following with your SQL Server credentials
    server = "your_server"
    database = "your_database"
    username = "your_username"
    password = "your_password"

    # Create an instance of IndexRebuilder
    index_rebuilder = IndexRebuilder(server, database, username, password)

    # Replace 123 with the desired given_id
    index_rebuilder.run(123)
