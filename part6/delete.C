#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string &relation,
					   const string &attrName,
					   const Operator op,
					   const Datatype type,
					   const char *attrValue)
{
	Status status;

	// For testing only
	// cout << "Deleting records from relation: " << relation << endl;
	// cout << "Attribute name: " << attrName << endl;

	if (attrName.empty())
	{
		// cout << "Deleting all records from relation: " << relation << endl;
		HeapFileScan scan(relation, status);
		if (status != OK)
		{
			return status;
		}

		status = scan.startScan(0, 0, STRING, NULL, EQ);
		if (status != OK)
		{
			return status;
		}

		RID rid;
		int deleteCount = 0;

		while (scan.scanNext(rid) == OK)
		{
			status = scan.deleteRecord();
			if (status != OK)
			{
				scan.endScan();
				return status;
			}
			deleteCount++;
		}

		scan.endScan();
		return OK;
	}

	// Testing only
	//  cout << "Attribute value: " << attrValue << endl;

	AttrDesc attrDesc;
	status = attrCat->getInfo(relation, attrName, attrDesc);
	if (status != OK)
	{
		return status;
	}

	// Convert attribute value to proper binary format
	char *filterVal = new char[attrDesc.attrLen];

	switch (type)
	{
	case INTEGER:
	{
		int val = atoi(attrValue);
		memcpy(filterVal, &val, sizeof(int));
		break;
	}
	case FLOAT:
	{
		float val = atof(attrValue);
		memcpy(filterVal, &val, sizeof(float));
		break;
	}
	case STRING:
	{
		memset(filterVal, 0, attrDesc.attrLen);
		strncpy(filterVal, attrValue, attrDesc.attrLen);
		break;
	}
	}

	// Create scanner and start filtered scan
	HeapFileScan scan(relation, status);
	if (status != OK)
	{
		delete[] filterVal;
		return status;
	}

	status = scan.startScan(attrDesc.attrOffset,
							attrDesc.attrLen,
							(Datatype)attrDesc.attrType,
							filterVal,
							op);

	if (status != OK)
	{
		delete[] filterVal;
		return status;
	}

	RID rid;
	// int deletedCount = 0;

	while (scan.scanNext(rid) == OK)
	{
		status = scan.deleteRecord();
		if (status != OK)
		{
			scan.endScan();
			delete[] filterVal;
			return status;
		}
		// deletedCount++;
	}

	scan.endScan();
	delete[] filterVal;

	// cout << "Deleted " << deletedCount << " records" << endl;
	return OK;

	return OK;
}
