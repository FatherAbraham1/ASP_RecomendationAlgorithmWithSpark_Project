from pyspark import SparkConf, SparkContext

def loadNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local[*]").setAppName("TopTenMovies")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadNames())

lines = sc.textFile("file:///ASP_RecomAlgoProject/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flip = movieCounts.map( lambda (x, y) : (y, x))
sortedMovies = flip.sortByKey(ascending = False)

sortedMoviesWithNames = sortedMovies.map(lambda (count, movie) : (nameDict.value[movie], count))

results = sortedMoviesWithNames.take(10)

for result in results:
    print result
