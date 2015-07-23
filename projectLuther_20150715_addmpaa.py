#TODO
#get rating, month of release, director, country, language
#create new feature of directors past ratings spread
#feature selection and extraction
#do regression one by one feature
#tranform features
#combine best features


import urllib
from bs4 import BeautifulSoup
from bs4.element import Tag
import pandas as pd
import numpy as np

def processMovie(url):
    moviePage = urllib.urlopen("http://www.imdb.com" + url)
    moviePageHTMLString = moviePage.read()
    
    # Parse string html into beautifulsoup parsed object
    moviePageSoup = BeautifulSoup(moviePageHTMLString)
    
    # Grab movie id from url
    movieID = url.split('/')[-2]    
    
    
    # Identify rating by its unique attribute of itemprop: contentrating
    try:    
        ratingBoxList = moviePageSoup.find_all('span', attrs={'itemprop': 'contentRating'})
        if len(ratingBoxList) > 0 and ratingBoxList[0].text.split()[1] != 'Rated':
            rating = ratingBoxList[0].text.split()[1]
        else:
            rating = ""
    except:
        rating = ""

        # Identify genreBox by its unique attribute of itemprop: genre
    genreBoxList = moviePageSoup.find_all('div', attrs={'itemprop': 'genre'})
    if len(genreBoxList) > 0:
        genreBox = genreBoxList[0]
        
        # Process genres into a list
        genresList = []
    
        # We are interested in the anchor tags that link to genres    
        for anchor in genreBox.find_all('a'):
            genresList.append(anchor.text)
        genres = ",".join(genresList)
        genres = genres.strip()
    else:
        genres = ""
    
    
    # Identify review scores
    reviewList = moviePageSoup.find_all('span', attrs={"itemprop": "ratingValue"})
    if len(reviewList) > 0:
        reviewBox = reviewList[0]
        review = float(reviewBox.text)
    else:
        review = np.nan

    
    # Identify metascore
    metascoreList = moviePageSoup.find_all('a', attrs={"href": "criticreviews?ref_=tt_ov_rt"})
    if len(metascoreList) > 0:
        metascoreBox = metascoreList[0]
        metascore = int(metascoreBox.text.strip().split('/')[0])
    else:
        metascore = np.nan
    
    
    # Identify budget
    budgetString = None
    for h4 in moviePageSoup.find_all('h4'):
        if h4.text == "Budget:":
            for s in h4.parent.stripped_strings:
                if '$' in s:
                    budgetString = s
                    break
    if budgetString:
        budget = int(s[1:].replace(',', ''))
    else:
        budget = np.nan
        
    # TODO: add to data frame instead of return
    return (movieID, genres, review, metascore, budget, rating)    
#    df = df.append({"ID": movieID, "Genre": genres, "Review": review,"Metascore": metascore,"Budget": budget},ignore_index=True)


#df = df[pd.notnull(df['EPS'])]
#df.dropna()  #drop all rows that have any NaN values


#It seems like it should be a simple thing: create an empty DataFrame in the Pandas Python Data Analysis Library. 
#But if you want to create a DataFrame that
#
#    is empty (has no records)
#    has datatypes
#    has columns in a specific order
#
#...i.e. the equivalent of SQL's CREATE TABLE, then it's not obvious how to do it in Pandas, and I wasn't able to find any one 
#web page that laid it all out. The trick is to use an empty Numpy ndarray in the DataFrame constructor:
#
#df=DataFrame(np.zeros(0,dtype=[
#('ProductID', 'i4'),
#('ProductName', 'a50')]))
#
#Then, to insert a single record:
#
#df = df.append({'ProductID':1234, 'ProductName':'Widget'})
#
#UPDATE 2013-07-18: Append is missing a parameter:
#
#df = df.append({'ProductID':1234, 'ProductName':'Widget'},ignore_index=True)

# Variables
year = 2013
pageTemplate = "http://www.imdb.com/search/title?at=0&sort=alpha&title_type=feature&year=%d,%d&start=%d"
movieNum = 5885     # Movie number we are currently processing
#totalMovies = 9308
totalMovies = 10

#TODO: check if these datatypes are ok
# Our global data frame
df = pd.DataFrame(np.zeros(0,dtype=[
('ID', 'a25'),#
('Genre', 'a50'),#
('Rating', 'a50'),#
('Review', 'f8'),#
('Metascore', 'i4'),#
('Budget', 'i4')]))#

    
# While we are not done processing all search pages ...
while movieNum != totalMovies:
    # Get search page HTML, starting with movieNum
    print "Start Processing %s" % (pageTemplate % (year, year, movieNum))
    searchPage = urllib.urlopen(pageTemplate % (year, year, movieNum))
    searchPageHTMLString = searchPage.read()
    
    # Parse string html into beautifulsoup parsed object
    searchPageSoup = BeautifulSoup(searchPageHTMLString)
    
    # Get table of results
    results = searchPageSoup.find_all("table", attrs={'class': 'results'})[0]
    
    # Loop thru only tags to find all tr's (table rows)
    print "Processing all movies on this page ..."
    for searchResult in results.children:
        # Check if this is a tag
        if isinstance(searchResult, Tag):
            # Get the table column for the movie        
            movieBox = searchResult.find_all('td', attrs={'class': 'title'})
            
            # Check that we actually found the box, and if so ...
            if len(movieBox) == 1:
                # ... find the link to the movie
                anchor = movieBox[0].findChild('a')
                
                # Process this movie URL (this function will scrape the data 
                #and insert into dataframe) for one movie
                print "\tProcessing movie with url %s" % anchor['href']
                movieID, genres, review, metascore, budget, rating = processMovie(anchor['href'])

                df = df.append({"ID": movieID, "Genre": genres, "Rating": rating,\
                "Review": review,"Metascore": metascore,"Budget": budget},ignore_index=True)
#                df.loc[movieNum] = [rowofdata]
#                print rowofdata
                print movieNum
                print "\tDone with this movie"
                
                #TODO: periodically save dataframe after processing
                if movieNum % 10 == 0:
                    df.to_csv("moviesdata150715_2013b.csv")
                
                # Mark that we processed one more movie
                movieNum+=1

dfclean = df.dropna()