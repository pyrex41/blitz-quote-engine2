import libsql_experimental as libsql
import readline
import os


def main():
    # Expand the home directory if needed
    file = os.path.expanduser("~/libsql-client-ts/packages/libsql-client/examples/local.db")

    # Use context manager for proper connection handling
    conn = libsql.connect(file)
    # Create table
    conn.execute("CREATE TABLE IF NOT EXISTS guest_book_entries (comment TEXT)")

    # Get user input
    comment = input("Enter your comment: ")

    # Insert the comment - convert list to tuple
    conn.execute("INSERT INTO guest_book_entries (comment) VALUES (?)", (comment,))

    conn.commit()

    # Query and display results
    print("\nGuest book entries:")
    cursor = conn.execute("SELECT * FROM guest_book_entries")
    for row in cursor.fetchall():
        print(f" - {row[0]}")


if __name__ == "__main__":
    main()

