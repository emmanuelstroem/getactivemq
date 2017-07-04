import csv # to write and read csv
import random # to randomise values for csv

# VARIABLES
# csv items
cellItems = ['A', 'G', 'C', 'T']
csvArray = []

first_row = 0
total_rows = 10

# shuffle array data to randomise it
def shuffleData(array):
    random.shuffle(array)
    return array

# generate a 5 element array to match rows
def generateRowArray(inputArray):
    allCellItems = []
    extraCellItem = random.choice(inputArray)
    shuffledCellItems = shuffleData(inputArray)

    for item in shuffledCellItems:
        allCellItems.append(item)

    # add random fifth element to array
    allCellItems.append(extraCellItem)

    return allCellItems

# create array of arrays with random values
def writeToCSV(desiredCSV):
    for i in range(0, total_rows):
        csvArrayItem = generateRowArray(cellItems)
        # print csvArrayItem
        csvArray.append(csvArrayItem)

    # create or update csv
    with open(desiredCSV, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile, dialect='excel', delimiter=',',\
        quotechar='|', quoting=csv.QUOTE_MINIMAL)

        for rows in csvArray:
            csv_writer.writerow(rows)