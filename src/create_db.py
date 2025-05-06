import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def main():
    database = "../db/goodreads-db.sqlite"

    sql_create_books_table = """ CREATE TABLE IF NOT EXISTS Books (
                                        BookID INTEGER PRIMARY KEY AUTOINCREMENT,
                                        Title TEXT,
                                        ISBN TEXT,
                                        ISBN13 TEXT,
                                        Language TEXT,
                                        PublicationYear INTEGER,
                                        Publisher TEXT,
                                        NumPages INTEGER
                                    ); """

    sql_create_authors_table = """ CREATE TABLE IF NOT EXISTS Authors (
                                        AuthorID INTEGER PRIMARY KEY AUTOINCREMENT,
                                        AuthorName TEXT
                                    ); """
    
    sql_create_book_authors_table = """ CREATE TABLE IF NOT EXISTS BookAuthors (
                                        BookID INTEGER,
                                        AuthorID INTEGER,
                                        PRIMARY KEY (BookID, AuthorID),
                                        FOREIGN KEY (BookID) REFERENCES Books(BookID),
                                        FOREIGN KEY (AuthorID) REFERENCES Authors(AuthorID)
                                    ); """
    
    sql_create_genres_table = """ CREATE TABLE IF NOT EXISTS Genres (
                                        GenreID INTEGER PRIMARY KEY AUTOINCREMENT,
                                        GenreName TEXT UNIQUE
                                    ); """
    
    sql_create_book_genres_table = """ CREATE TABLE IF NOT EXISTS BookGenres (
                                        BookID INTEGER,
                                        GenreID INTEGER,
                                        PRIMARY KEY (BookID, GenreID),
                                        FOREIGN KEY (BookID) REFERENCES Books(BookID),
                                        FOREIGN KEY (GenreID) REFERENCES Genres(GenreID)
                                    ); """
    
    sql_create_users_table = """ CREATE TABLE IF NOT EXISTS Users (
                                        UserID INTEGER PRIMARY KEY AUTOINCREMENT,
                                        UserName TEXT,
                                        Location TEXT
                                    ); """
    
    sql_create_ratings_table = """ CREATE TABLE IF NOT EXISTS Ratings (
                                        UserID INTEGER,
                                        BookID INTEGER,
                                        Rating INTEGER CHECK (Rating >= 1 AND Rating <= 5),
                                        PRIMARY KEY (UserID, BookID),
                                        FOREIGN KEY (UserID) REFERENCES Users(UserID),
                                        FOREIGN KEY (BookID) REFERENCES Books(BookID)
                                    ); """
    
    sql_create_reviews_table = """ CREATE TABLE IF NOT EXISTS Reviews (
                                        UserID INTEGER,
                                        BookID INTEGER,
                                        Rating INTEGER CHECK (Rating >= 1 AND Rating <= 5),
                                        PRIMARY KEY (UserID, BookID),
                                        FOREIGN KEY (UserID) REFERENCES Users(UserID),
                                        FOREIGN KEY (BookID) REFERENCES Books(BookID)
                                    ); """

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create books table
        create_table(conn, sql_create_books_table)

        # create authors table
        create_table(conn, sql_create_authors_table)
        
        # create book authors table
        create_table(conn, sql_create_book_authors_table)
        
        # create genres table
        create_table(conn, sql_create_genres_table)
        
        # create book genres table
        create_table(conn, sql_create_book_genres_table)

        # create users table
        create_table(conn, sql_create_users_table)
       
        # create ratings table
        create_table(conn, sql_create_ratings_table)
        
        # create reviews table
        create_table(conn, sql_create_reviews_table)
    else:
        print("Error! cannot create the database connection.")

if __name__ == '__main__':
    main()