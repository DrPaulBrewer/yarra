# Yet Another Robotic Research Assistant (YARRA)

for backend processing of economic simulations using single-market-robot-simulator and related packages.

## Overview

    const storage = require('@google-cloud/storage')(optional_api_key);
    const yarra = require('yarra')(storage, trueForVerbose );

Configure yarra's buckets before asking it to do anything.

    yarra.setBuckets({ sim: 'bucket-for-simulation-results',
    		       study: 'bucket-for-study-zip-results',
		       lock: 'bucket-for-lockfiles'
		       });


## Tasks

Currently the only task yarra knows about is zipperTask

    yarra.zipperTask()


returns a Promise to a single pass of a task to locate finished, verified studies, zip up the results to the study bucket from the sim bucket, and cleanup.

## Grunt?

At this time there is no usage of or special support for grunt, but that might change in the future.

## Copyright

Copyright 2017 Paul Brewer, Economic and Financial Technology Consulting LLC <drpaulbrewer@eaftc.com>

## License

The MIT License




