import pandas as pd
import numpy as np
from objects import Author, Book, Genre
import db_functions
from utils import remove_extra_spaces, list_flatten
import os

## Data import
df = pd.read_csv("../data/Goodreads_books_with_genres.csv")
df = df.drop(axis=0, index=8180) # parche
df = df.drop(axis=0, index=11098) # parche
df['publication_date'] = pd.to_datetime(df['publication_date'], format='%m/%d/%Y')
df['Author'] = df['Author'].str.replace('"',"'")

# abrir conexion
db_functions.connect()

# Fill authors table
series_authors = df['Author'] # column of interest
list_authors = series_authors.str.split(pat="/").values.tolist() # convert to list (separated)
flat_list_authors = list_flatten(list_authors) # list flatten
array_authors_unique = pd.Series(flat_list_authors).unique()

for element in array_authors_unique:
    element = remove_extra_spaces(element)
    author = Author(author_name=element)
    db_functions.add_author(author=author)
print("Authors table filled!")

# Fill genres table
series_genres = df['genres'].dropna() # column of interest
list_genres = series_genres.str.split(pat=";").values.tolist() # convert to list (separated)
flat_list_genres = list_flatten(list_genres) # list flatten
flat_list_genres_cap = [x.capitalize().strip() for x in flat_list_genres]
array_genres_unique = pd.Series(flat_list_genres_cap).unique()

for element in array_genres_unique:
    #element = remove_extra_spaces(element)
    genre = Genre(genre_name=element)
    db_functions.add_genre(genre=genre)
print("Genre table filled!")


# Fill books table

book_list = list(zip(df['Book Id'],df['Title'],df['isbn'],
                       df['isbn13'],df['language_code'],df['publication_date'].dt.year,
                       df['publisher'], df['num_pages']))

for element in book_list:
    book = Book(bookid=element[0],
                title=element[1],
                isbn=element[2],
                isbn13=element[3],
                language=element[4],
                publication_year=element[5],
                publisher=element[6],
                num_pages=element[7])
    db_functions.add_book(book=book)
print("Book table filled!")

### Fill BookAuthor Table

for isbn in df['isbn']:
    book_id = db_functions.get_bookid_from_isbn(isbn)
    
    for authors_name in df[df['isbn']==isbn]['Author']:
        for author_name in authors_name.split(sep='/'):
            author_name = remove_extra_spaces(author_name)
            author_id = db_functions.get_authorid_from_authorname(author_name)
            db_functions.add_book_author(book_id=book_id, author_id=author_id)

print("Book Author Table Filled!")


## Close connection to database
db_functions.close()