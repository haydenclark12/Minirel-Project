#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	// Get relation
	RelDesc relDesc;
	Status status = relCat->getInfo(relation, relDesc);
	if (status != OK) {
		cerr << "Error: Unable to get relation information." << endl;
		return status;
	}

	// Check if every attribute has is the correct type, length, and not null
	if (attrCnt != relDesc.attrCnt) {
		cerr << "Error: Attribute count does not match." << endl;
		return BADCATPARM;
	}

	// Sort attrList so that it matches the order in the catalog
	AttrDesc attrDesc;
	attrInfo sortedAttrList[attrCnt];
	int found = 0;
	int newRecLen = 0;
	for (int i = 0; i < attrCnt; i++) {
		// Minirel doesn't support NULL
		if (attrList[i].attrValue == NULL) {
			cerr << "Error: Attribute value is NULL." << endl;
			return BADCATPARM;
		}
		for (int j = 0; j < attrCnt; j++) {
			status = attrCat->getInfo(relation, attrList[j].attrName, attrDesc);
			if (status != OK) {
				cerr << "Error: Unable to get attribute information." << endl;
				return status;
			}
			if (strcmp(attrList[i].attrName, attrDesc.attrName) == 0) {
				// Check to see if attribute type and length matches
				if (attrList[i].attrLen > attrDesc.attrLen) {
					cerr << "Error: Attribute length too long for "
						<< attrList[i].attrName << endl;
					return ATTRTOOLONG;
				}
				if (attrList[i].attrType != attrDesc.attrType) {
					cerr << "Error: Attribute type mismatch for "
						<< attrList[i].attrName << endl;
					return ATTRTYPEMISMATCH;
				}

				sortedAttrList[j] = attrList[i];
				newRecLen += attrDesc.attrLen;
				found = 1;
				break;
			}
		}
		if (found == 0) {
			cerr << "Error: Attribute " << attrList[i].attrName
				<< " not found in relation " << relation << endl;
			return ATTRNOTFOUND;
		}
		found = 0;
	}

	// Create a new record for insertion
	Record rec;
	rec.length = newRecLen;
	rec.data = new char[newRecLen];
	if (rec.data == NULL) {
		cerr << "Error: Unable to allocate memory for record." << endl;
		return UNIXERR;
	}

	// Initialize the record data to zero
	memset(rec.data, 0, newRecLen);

	// Set the record data
	for (int i = 0; i < attrCnt; i++) {
		// Get the attribute description
		status = attrCat->getInfo(relation, attrList[i].attrName, attrDesc);
		if (status != OK) {
			cerr << "Error: Unable to get attribute information." << endl;
			delete[] rec.data;
			return status;
		}

		// Set the attribute value
		switch (attrDesc.attrType) {
			case INTEGER: {
				int val = atoi((char *) attrList[i].attrValue);
				memcpy(rec.data + attrDesc.attrOffset, &val, sizeof(int));
				break;
			}
			case FLOAT: {
				float val = atof((char *) attrList[i].attrValue);
				memcpy(rec.data + attrDesc.attrOffset, &val, sizeof(float));
				break;
			}
			case STRING: {
				memset(rec.data + attrDesc.attrOffset, 0, attrDesc.attrLen);
				strncpy((char *) rec.data + attrDesc.attrOffset,(char *) attrList[i].attrValue, attrDesc.attrLen);
				break;
			}
			default:
				cerr << "Error: Unknown attribute type." << endl;
				delete[] rec.data;
				return BADCATPARM;
		}
	}

	// Insert the record into the relation
	InsertFileScan inserter(relation, status);
	if (status != OK) {
		cerr << "Error: Unable to open heap file." << endl;
		delete[] rec.data;
		return status;
	}

	RID rid;
	status = inserter.insertRecord(rec, rid);
	if (status != OK) {
		cerr << "Error: Unable to insert record." << endl;
		delete[] rec.data;
		return status;
	}

	return status;
}

