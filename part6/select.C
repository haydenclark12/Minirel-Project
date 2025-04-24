#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    // cout << "Doing QU_Select " << endl;

	// Create an array of projection descriptions for each projection
	AttrDesc* projDescs = new AttrDesc[projCnt];

	// Keep track of record length
	int reclen = 0;

	// Iterate through all projections and save descriptions to projDesc
	for (int i = 0; i < projCnt; i++)
	{
		Status status = attrCat->getInfo(projNames[i].relName,projNames[i].attrName,projDescs[i]);

		if (status != OK)
		{
			delete[] projDescs;
			return status;
		}
		reclen += projDescs[i].attrLen;
	}

	// If no WHERE clause we can call ScanSelect with NULL
	if (attr == NULL) 
	{
		return ScanSelect(result,projCnt,projDescs,NULL,op,NULL,reclen);
	}

	// If there is a WHERE clause we can call ScanSelect with AttrDesc for selection attribute
	AttrDesc attrDesc;
	Status status = attrCat->getInfo(attr->relName,attr->attrName,attrDesc);
	if (status != OK) 
	{
		delete[] projDescs;
		return status;
	}

	// Convert attrValue into binary form
	char* filterVal = new char[attrDesc.attrLen];

	switch(attr->attrType)
	{
		case INTEGER: 
		{
			// Convert the value into an int and store
			int val = atoi(attrValue);
			memcpy(filterVal,&val,sizeof(int));
			break;
		}
		case FLOAT: 
		{
			float val = atof(attrValue);
			memcpy(filterVal,&val,sizeof(float));
			break;
		}
		case STRING:
		{
			memset (filterVal,0,attrDesc.attrLen);
			strncpy(filterVal,attrValue,attrDesc.attrLen);
			break;
		}
	}

	// Call ScanSelect
    status = ScanSelect(result, projCnt, projDescs, &attrDesc, op, filterVal, reclen);

	delete[] projDescs;
    delete[] filterVal;

    return status;
	



}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    // cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;
	HeapFileScan scan(projNames[0].relName,status);

	// Start the scan and check for WHERE clause
	if (attrDesc != NULL)
	{
		// If we have a WHERE clause
		status = scan.startScan(
			attrDesc->attrOffset,
			attrDesc->attrLen,
			(Datatype)attrDesc->attrType,
			filter,
			op
		);
	}else
	{
		// If there is no WHERE clause
		status = scan.startScan(0,0,STRING,NULL,EQ);
	}

	if (status != OK) return status;

	// Open the result table for inserting
	InsertFileScan resultInserter(result,status);
	if (status != OK)
	{
		scan.endScan();
		return status;
	}

	// Loop through matching records
	RID rid;
	while ((status = scan.scanNext(rid)) == OK)
	{
		// Get record of matching tuple
		Record rec;
		status = scan.getRecord(rec);
		if (status != OK) {
			scan.endScan();
			return status;
		}

		// Allocate buffer for projected tuple
		char* newTuple = new char[reclen];
		int offset = 0;

		// Project each attribute
		for (int i = 0; i < projCnt; ++i){
			// Get the attribute data
			const AttrDesc& projAttr = projNames[i];

			// Get pointer to the data of the attribute in the record
			void* src = (char*)rec.data + projAttr.attrOffset;

			// Copy the attribute data into newTuple
			memcpy(newTuple + offset,src,projAttr.attrLen);

			// Increment offset as to not overwrite any data
			offset += projAttr.attrLen;
		}


		// Store our projected tuple into a new Record and insert it into the result file
		Record projected;
		projected.data = newTuple;
		projected.length = reclen;

		RID dummy;
		status = resultInserter.insertRecord(projected,dummy);
		
		// Memory management
		delete[] newTuple;

		if (status != OK) {
			scan.endScan();
			return status;
		}
	}
	scan.endScan();
	return OK;
}
