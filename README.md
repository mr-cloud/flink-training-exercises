This repository contains reference solutions and utility classes for the Flink Training exercises 
on [http://training.data-artisans.com](http://training.data-artisans.com).

### Schema of datasets
- ride

        rideId         : Long      // a unique id for each ride
        taxiId         : Long      // a unique id for each taxi
        driverId       : Long      // a unique id for each driver
        isStart        : Boolean   // TRUE for ride start events, FALSE for ride end events
        startTime      : DateTime  // the start time of a ride
        endTime        : DateTime  // the end time of a ride,
                                   //   "1970-01-01 00:00:00" for start events
        startLon       : Float     // the longitude of the ride start location
        startLat       : Float     // the latitude of the ride start location
        endLon         : Float     // the longitude of the ride end location
        endLat         : Float     // the latitude of the ride end location
        passengerCnt   : Short     // number of passengers on the ride
- fare

        rideId         : Long      // a unique id for each ride
        taxiId         : Long      // a unique id for each taxi
        driverId       : Long      // a unique id for each driver
        startTime      : DateTime  // the start time of a ride
        paymentType    : String    // CSH or CRD
        tip            : Float     // tip for this ride
        tolls          : Float     // tolls for this ride
        totalFare      : Float     // total fare collected