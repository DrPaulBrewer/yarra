/* Copyright (C) 2017 Paul Brewer Economic and Financial Technology Consulting LLC <drpaulbrewer@eaftc.com> */
/* All Rights Reserved */
/* jshint esnext:true,eqeqeq:true,undef:true,lastsemic:true,strict:true,unused:true,node:true */

"use strict";

const pEachSeries = require('p-each-series');
const Study = require('single-market-robot-simulator-study');
const promiseRetry = require('promise-retry');
const backoffStrategy = {
    retries: 3,
    factor: 2,
    minTimeout:  1000,
    maxTimeout: 10000,
    randomize: true
};


module.exports = function yarra(storage, progress){

    const closure = {};


    const verify = require('verify-bucket-md5')(storage);
    const zipBucket = require('zip-bucket')(storage);
    const vaporlock = require('vaporlock')(storage, 60*60*1000);

    closure.verify = verify;
    closure.zipBucket = zipBucket;
    closure.vaporlock = vaporlock;
    

    const buckets = {};

    closure.buckets = buckets;

    /**
     * set buckets to be used in various functions
     * 
     * @param {Object} newBuckets
     * @return {Object} a copy of the yarra closure, useful for method chaining
     */

    function setBuckets(newBuckets){
	Object.assign(buckets, newBuckets);
	return closure;
    }

    closure.setBuckets = setBuckets;


    function show(x){
	if (progress)
	    console.log("progress: "+JSON.stringify(x));
	return x;
    }

//    function ignore(){}
    
    /**
     * pass single parameter x through to return and emit as console.log(x)
     *
     * @param {Any} x
     * @return {Any} x
     */

    function log(x){
	console.log(JSON.stringify(x));
	return x;
    }

    closure.log = log;

    /**
     * download a bucket file and parse JSON
     *
     * @param {string} bucket 
     * @param {string} path path in bucket
     * @return {Any} object parsed from file data
     */

    function dlJSON(bucket, path){
	return (promiseRetry((retry)=>(storage
				       .bucket(bucket)
				       .file(path)
				       .download()
				       .catch(retry)), backoffStrategy)
		.then((buf)=>(buf.toString('utf8')))
		.then((jsonstring)=>(JSON.parse(jsonstring)))
	       );
    }

    closure.dlJSON = dlJSON;

    /**
     * obtain a listing of files (with metadata) from a bucket
     * 
     * @param {string} bucket
     * @param {string} [path] optional - matches file names beginning with path
     * @return {Promise<Object>} object with file path as key and file metadata object as value
     */

    function ls(bucketName,path){
	const dir = {};
	if (typeof(bucketName)!=='string')
	    throw new Error("expected string bucketName, got: "+JSON.stringify(bucketName));
	if (bucketName.length===0)
	    throw new Error("invalid zero length bucketName");
	return (promiseRetry((retry)=>(storage
				       .bucket(bucketName)
				       .getFiles((path) && ({prefix: path}))
				       .catch(retry)), backoffStrategy)
		.then((data)=>(data[0]))
		.then((files)=>{
		    files.forEach((f)=>{
			if (f && (f.name))
			    dir[f.name] = {
				name: f.name,
				metadata: f.metadata
			    };
		    });
		    return dir;
		})
	       );
    }

    closure.ls = ls;

    /**
     * determine if all paths exist in the listing
     *
     * @param {string[]} paths
     * @param {Object} lsdir a directory listing obtained from ls()
     * @return {boolean} true if all paths exist in the listing
     */

    function allExist(paths, lsdir){
	return paths.every((f)=>(lsdir.hasOwnProperty(f)));
    }

    closure.allExist = allExist;

    /**
     * determine if all of a set of conditions are truthy
     * equivalent to .every((c)=>(c))
     *
     * @param {boolean[]} conditions to test
     * @return {boolean} true if all conditions are true
     */

    function allTrue(conditions){
	let i = conditions.length;
	let c = true;
	while ( (c) && (i-->0) ){
	    c = (c && conditions[i]);
	}
	return c;
    }

    closure.allTrue = allTrue;

    /**
     * determines if all md5-verifiable files exist for a study and pass md5 check 
     * 
     * @param {string} pathToStudyJSON /path/to/study/config.json
     * @param {Object} dir A "directory" object from yarra.ls()
     * @return {string} returns pathToStudyJSON if verified, otherwise throws
     * @throws Will throw "not read, study:" for missing files or "md5 verification failed" when appropriate 
     */
     
    function verifyStudy(pathToStudyJSON, dir){
	if (progress) console.log("verifying: "+pathToStudyJSON);
	const md5jsonfiles=[];
	return (dlJSON(buckets.sim,pathToStudyJSON)
		.then(show)
		.then(function(study){
		    [].push.apply(md5jsonfiles,Study.paths(pathToStudyJSON,study.configurations.length,"md5.json"));
		    return allExist(md5jsonfiles, dir);
		})
		.then(show)
		.then(function(studyReady){
		    if (!studyReady) throw new Error("not ready, study: "+pathToStudyJSON);
		})
		.then(function(){
		    return (Promise
			    .all(md5jsonfiles.map((f)=>(verify(buckets.sim,f).then(show,show))))
			    .then((results)=>(results.map((x)=>(x && x[0]))))
			    .then(allTrue)
			    .then((ok)=>{
				if (!ok)
				    throw new Error("md5 verification failed for study:"+pathToStudyJSON);
				else
				    return pathToStudyJSON;
			    })
			   );
		})
	       );
    }

    closure.verifyStudy = verifyStudy;

    /**
     * creates a .zip copy of a study 
     *  copies from buckets.sim directory with pathToStudyJSON and all subdirs
     *  copies to a buckets.study .zip file automatically named from directory name in bucket.sim
     * 
     * @param {string} pathToStudyJSON /path/to/study/config.json
     * @return {Promise<Object>} a promise that resolves to the results of the zipBucket call, see npm:zip-bucket
     */

    function zip(pathToStudyJSON){
	return (vaporlock(buckets.lock, pathToStudyJSON)
		.then(()=>(zipBucket({
		    fromBucket: buckets.sim,
		    fromPath: pathToStudyJSON.replace("config.json",""),
		    toBucket: buckets.study,
		    toPath: pathToStudyJSON.replace("/config.json",".zip"),
		    progress
		})))
	       );
    }

    closure.zip = zip;

    /**
     * creates an object {dir, studies} from input dir and a property "studies" containing a list of paths to study config.json files from a dir=yarra.ls()
     * 
     * @param {Object} dir A directory generated by yarra.ls()
     * @return {Object} an object with fields dir and studies.  Studies is an array of strings that are file pathos of config.json files.
     */

    function studies(dir){
	return { dir, studies: Object.keys(dir).filter((fname)=>(fname.endsWith("config.json"))) };
    }

    closure.studies = studies;

    /**
     * passes the input object and adds a property verifiedStudies containing all studies that pass the existence/md5 tests
     * 
     * @param {object} obj with properties obj.dir and obj.studies
     * @return {object} same object with additional property obj.verifiedStudies
     */

    function verifyAll(obj){
	return (Promise
	 .all(obj.studies.map((s)=>(verifyStudy(s,obj.dir).catch(log))))
	 .then((verifiedStudies)=>{
	     obj.verifiedStudies = verifiedStudies.filter((s)=>((typeof(s)==='string') && (s.endsWith("config.json"))));
	     return obj;
	 })
	);
    }

    closure.verifyAll = verifyAll;

    /**
     * returns a Promise to an object containing .dir, .studies, and .verifiedStudies fields
     *
     * @return {Promise<object>} Promise resolves to object containing .dir, .studies, and .verifiedStudies fields
     */

    function allVerifiedStudies(){
	if (!buckets.sim)
	    throw new Error("buckets.sim is empty");
	return (ls(buckets.sim)
		.then(studies)
		.then(verifyAll)
	       );		
    }

    closure.allVerifiedStudies = allVerifiedStudies;

    /**
     * delete a study and associated lockfile
     *
     * @return {Promise} promise resolves when deletion is complete, or throws error
     */

    function deleteStudy(pathToStudyJSON){
	const prefix = pathToStudyJSON.replace("config.json","");
	if ((prefix.length===0) || (prefix === '/'))
	    throw new Error("will not delete entire bucket, pathToStudyJSON: "+pathToStudyJSON);
	return (promiseRetry((retry)=>(storage
				       .bucket(buckets.sim)
				       .deleteFiles({prefix,
						     force: true })
				       .catch(retry)), backoffStrategy)
		.then(()=>{
		    return promiseRetry((retry)=>(storage
						  .bucket(buckets.lock)
						  .file(pathToStudyJSON)
						  .delete()
						  .catch(retry)), backoffStrategy);
		})
	       );
    }

    closure.deleteStudy = deleteStudy;

    /** runs a single pass of zipperTask.  Scans for verifiedStudies in buckets.sim, zips to buckets.study, deleteds.
     *
     * @return {Promise}
     */

    function zipperTask(){
	return (allVerifiedStudies()
		.then((obj)=>{
		    return (pEachSeries(obj.verifiedStudies, (study)=>{
			return (zip(study)
				.then(()=>(deleteStudy(study)))
				.then(()=>{
				    console.log("zipped and deleted: "+study);
				}, (e)=>{
				    console.log("error processing: "+study);
				    console.log(e);
				})
			       );
		    })
			   );
		})
	       );
    }

    closure.zipperTask = zipperTask;

    return closure;

};
