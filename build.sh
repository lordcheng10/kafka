#!/bin/bash

gradle
#./gradlew srcJar
#./gradlew aggregatedJavadoc
#./gradlew test # runs both unit and integration tests
#./gradlew unitTest
#./gradlew integrationTest
./gradlew clean
./gradlew releaseTarGz
