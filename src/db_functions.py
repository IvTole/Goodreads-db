# DATABASE LAYER

import sqlite3 as sql
from contextlib import closing
from objects import Author, Book, Genre

# Variable global que representa el string de conexion
conn = None

# Vamos a necesitar varios métodos
# Autores

def connect():
    global conn
    if not conn: # if conn has not been set then set
        conn =sql.connect("../db/goodreads-db.sqlite")
        conn.row_factory = sql.Row # returns a dict instead of a tuple
        
def close():
    if conn:
        conn.close()
    # we call from de ui moduls when the user terminate the application

### Author functions

def add_author(author): # needs an object that represents all of the information of the author
    sql_query = '''INSERT OR IGNORE INTO Authors (AuthorName)
    VALUES (?)'''
    with closing(conn.cursor()) as cursor:
        cursor.execute(sql_query, (author.author_name,)) # representa al objeto author
        conn.commit()

def get_authorid_from_authorname(author_name): 
    query = ''' SELECT AuthorID from Authors WHERE AuthorName=?'''
    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (author_name,))
        results = cursor.fetchone()
    return results['AuthorID']

### Book functions

def add_book(book): # needs an object that represents all of the information of the author
    sql_query = '''INSERT OR IGNORE INTO Books (BookID, Title, ISBN, ISBN13, Language, PublicationYear, Publisher, NumPages)
    VALUES (?,?,?,?,?,?,?,?)'''
    with closing(conn.cursor()) as cursor:
        cursor.execute(sql_query, (book.bookid, book.title, book.isbn, book.isbn13,
                                   book.language, book.publication_year, book.publisher,
                                   book.num_pages)) # representa al objeto book
        conn.commit()

def get_bookid_from_bookname(book_name): 
    query = ''' SELECT BookID from Books WHERE Title=?'''
    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (book_name,))
        results = cursor.fetchone()
    return results['BookID']

def get_bookid_from_isbn(isbn): 
    query = ''' SELECT BookID from Books WHERE ISBN=?'''
    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (isbn,))
        results = cursor.fetchone()
    return results['BookID']

### Genre functions

def add_genre(genre): # needs an object that represents all of the information of the genre
    sql_query = '''INSERT OR IGNORE INTO Genres (GenreName)
    VALUES (?)'''
    with closing(conn.cursor()) as cursor:
        cursor.execute(sql_query, (genre.genre_name,)) # representa al objeto genre
        conn.commit()

## BookAuthor functions

def add_book_author(book_id, author_id): 
    sql_query = '''INSERT OR IGNORE INTO BookAuthors (BookID, AuthorID)
    VALUES (?,?)'''
    with closing(conn.cursor()) as cursor:

        try:
            cursor.execute(sql_query, (book_id, author_id)) # representa los id's del libro y el autor
            conn.commit()
        except:
            print('Not found')