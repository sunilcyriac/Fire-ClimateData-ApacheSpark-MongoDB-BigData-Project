
/*
FIT5148 Distributed databases and big data
Team Members:
    - HamidReza Mohammadian Dehkordi 28855205
    - Sunil Cyriac 29003164
Lab: Tuesday 12pm-2pm B214 Huashun Li
*/

/*
TaskA.1
*/
//***** Please replace the CSV file directory after --file in the command below
// Example : --file /Users/huashun/Desktop/ClimateData-part1.csv
// The reason w used while and cursor is for some queries we didn't get any output with pretty() so we used "while"
// "cursor" for all of them.

/*
Warning: Please make sure the database name is fit5148_db and the collection names are climate and fire

run the "$mongo as2TaskA.js" command under the same directory of javascript
*/

//mongoimport -d fit5148_db -c fire --type csv --file /Users/hamidmo/Downloads/FireData-Part1.csv --headerline
//mongoimport -d fit5148_db -c climate --type csv --file /Users/hamidmo/Downloads/ClimateData-Part1.csv --headerline

//Connecting to Mongodb
var db = connect("localhost:27017/fit5148_db");
allMadMen = null

print("#######################   taskA.2   ##########################")
print("Find climate data on 15th December 2017" + "\n")

allMadMen = db.climate.find({"Date":"2017-05-15"});
while (allMadMen.hasNext()) {
  printjson(allMadMen.next())
}
print("**************************************************************")
print("#######################   taskA.3   ##########################")
print("Find the latitude, longitude and confidence when the surface temperature (°C) was between 65 °C and 100 °C."+"\n")

cursor = db.fire.find(
  {"$and":[
      {"Surface Temperature (Celcius)":{"$gte":65}},
      {"Surface Temperature (Celcius)":{"$lte":100}}
    ]
  },
  {"Latitude":1,"Longitude":1,"Confidence":1, "Surface Temperature (Celcius)":1,"_id":0}
);
while ( cursor.hasNext() ) {
   printjson( cursor.next() )
   print(",");
}

print("**************************************************************")
print("#######################   taskA.4   ##########################")
print("Find surface temperature (°C), air temperature (°C), relative humidity and maximum wind speed on 15th and 16th of December 2017."+ "\n")

cursor = db.fire.aggregate(
  {
    $match:
      {$or:[
        {"Date":"2017-12-15"},
        {"Date":"2017-12-16"}
      ]
    }
  },
  {
    $lookup:{
      from:"climate",
      localField: "Date",
      foreignField: "Date",
      as: "Climate"
    }
  },
  {
    $project:{
      "Date":1,
      "Surface Temperature (Celcius)":1,"Climate.Air Temperature(Celcius)":1,
      "Climate.Relative Humidity":1, "Climate.Max Wind Speed":1,"_id":0
    }
  }
);
while ( cursor.hasNext() ) {
   printjson( cursor.next() )
   print(",");
}

print("**************************************************************")
print("#######################   taskA.5   ##########################")
print("Find datetime, air temperature (°C), \
surface temperature (°C) and confidence when the confidence is between 80 and 100."+"\n")

cursor = db.fire.aggregate(
  {
    $match:
      {$and:[
        {"Confidence":{"$gte":80}},
        {"Confidence":{"$lte":100}}
      ]
    }
  },
  {
    $lookup:{
      from:"climate",
      localField: "Date",
      foreignField: "Date",
      as: "Climate"
    }
  },
  {
    $project:{
      "Confidence":1,"Datetime":1,"Climate.Air Temperature(Celcius)":1,
      "Surface Temperature (Celcius)":1, "_id":0
    }
  }
);
while ( cursor.hasNext() ) {
   printjson( cursor.next() )
   print(",");
}

print("**************************************************************")
print("#######################   taskA.6   ##########################")
print("Find top 10 records with highest surface temperature (°C)."+"\n")

cursor = db.fire.find().sort({"Surface Temperature (Celcius)":-1}).limit(10)
while ( cursor.hasNext() ) {
   printjson( cursor.next() )
   print(",");
}

print("**************************************************************")
print("#######################   taskA.7   ##########################")
print("Find the number of fire in each day. You are required to \
only display total number of fire and the date in the output."+"\n")

cursor = db.fire.aggregate(
  {$group:
    {_id:"$Date", numberOfFires:{$sum:1}}
  }
);
while ( cursor.hasNext() ) {
   printjson( cursor.next() )
   print(",");
}


print("**************************************************************")
print("#######################   taskA.8   ##########################")
print("Find the average surface temperature (°C) for each day.\
 You are required to only display average surface temperature (°C) and the date in the output."+"\n")

cursor = db.fire.aggregate(
  {$group:
    {_id:"$Date", AverageSurfaceTemp:{$avg:"$Surface Temperature (Celcius)"}}
  }
);
while ( cursor.hasNext() ) {
   printjson( cursor.next() )
   print(",");
}

print("End of TaskA")
