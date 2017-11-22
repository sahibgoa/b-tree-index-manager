/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/page_not_pinned_exception.h"


//#define DEBUG

namespace badgerdb
{
	// -----------------------------------------------------------------------------
	// BTreeIndex::BTreeIndex -- Constructor
	// -----------------------------------------------------------------------------
	BTreeIndex::BTreeIndex(
		const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType) {

			// Create index file name
			std::ostringstream idxStr;
			idxStr << relationName << '.' << attrByteOffset;
			outIndexName = idxStr.str();

			// initialize btree index variables
			bufMgr = bufMgrIn;
			headerPageNum = 1;
			attributeType = attrType;
			this->attrByteOffset = attrByteOffset;
			leafOccupancy = 0;
			nodeOccupancy = 0;
			scanExecuting = false;

			IndexMetaInfo* metadata;
			Page* headerPage;
			Page* rootPage;

			try {
				// Create file, check if it exists
				file = new BlobFile(outIndexName, true);
				// File does not exist, so new index file has been created

				// Allocate index meta info page and btree root page
				bufMgr->allocPage(file, headerPageNum, headerPage);
				bufMgr->allocPage(file, rootPageNum, rootPage);

				// Set up index meta info
				metadata = (IndexMetaInfo*) headerPage;
				strcpy(metadata->relationName, relationName.c_str());
				metadata->attrByteOffset = attrByteOffset;
				metadata->attrType = attrType;
				metadata->rootPageNo = rootPageNum;

				// Set up the root of the btree
				NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage;
				root->level = 1;
				for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
					root->keyArray[i] = -1;
					root->pageNoArray[i] = Page::INVALID_NUMBER;
				}
				root->pageNoArray[INTARRAYNONLEAFSIZE] = Page::INVALID_NUMBER;

				// Scan relation and insert entries for all tuples into index
				try {
					FileScan fileScan(relationName, bufMgr);
					RecordId rid;
					while (true) {
						fileScan.scanNext(rid);
						insertEntry((int*) fileScan.getRecord().c_str() + attrByteOffset, rid);
					}
				} catch (EndOfFileException e) {
					// Do nothing. Finished scanning file.
				}

				// Unpin header page and root page as they are no longer in use
				try {
					bufMgr->unPinPage(file, headerPageNum, true);
				} catch (PageNotPinnedException e) {
					// Do nothing.
				}
				try {
					bufMgr->unPinPage(file, rootPageNum, true);
				} catch (PageNotPinnedException e) {
					// Do nothing.
				}
			} catch (FileExistsException e) {  // File exists
				// Open the file
				file = new BlobFile(outIndexName, false);

				// Get index meta info for value checking
				bufMgr->readPage(file, headerPageNum, headerPage);
				metadata = (IndexMetaInfo*) headerPage;

				// Check that values in (relationName, attribute byte, attribute type etc.) match parameters
				if (strcmp(metadata->relationName, relationName.c_str()) != 0
					|| metadata->attrByteOffset != attrByteOffset
					|| metadata->attrType != attrType) {
						// Metadata does not match the parameters
						// Unpin header page before exiting
						try {
							bufMgr->unPinPage(file, headerPageNum, false);
						} catch (PageNotPinnedException e) {
							// Do nothing.
						}
						throw BadIndexInfoException("Error: Existing index metadata does not match parameters passed.");
				}
				// Metatdata matches
				// Unpin header page
				try {
					bufMgr->unPinPage(file, headerPageNum, false);
				} catch (PageNotPinnedException e) {
					// Do nothing.
				}
			}
		}


		// -----------------------------------------------------------------------------
		// BTreeIndex::~BTreeIndex -- destructor
		// -----------------------------------------------------------------------------
		BTreeIndex::~BTreeIndex() {
		}

		// -----------------------------------------------------------------------------
		// BTreeIndex::insertEntry
		// -----------------------------------------------------------------------------

		// TODO: sahib
		void BTreeIndex::insertEntry(const void *key, const RecordId rid) {

		}

		// -----------------------------------------------------------------------------
		// BTreeIndex::startScan
		// -----------------------------------------------------------------------------

		// TODO: sreejita
		void BTreeIndex::startScan(const void* lowValParm,
			const Operator lowOpParm,
			const void* highValParm,
			const Operator highOpParm) {

		}

		// -----------------------------------------------------------------------------
		// BTreeIndex::scanNext
		// -----------------------------------------------------------------------------
		void BTreeIndex::scanNext(RecordId& outRid) {
				// Check that scan has successfully started
				if (!scanExecuting) {
					throw ScanNotInitializedException();
				}

        // Keep track of node being evaluated
        LeafNodeInt* currentNode = (LeafNodeInt*) currentPageData;

				// Look for record id of next matching tuple
				while (true) {
					// Validate index of entry to be evaluated
					if (nextEntry == INTARRAYLEAFSIZE) {
						// Unpin page since no more entries to be scanned on this leaf page
            try {
              bufMgr->unPinPage(file, currentPageNum, false);
            } catch (PageNotPinnedException e) {
              // Do nothing.
            }

            // Move to right sibling leaf page
						PageId rightSibPageNo = currentNode->rightSibPageNo;

						// Check that the right sibling is a valid leaf page
						if (rightSibPageNo == 0) {
							// No more entries to be scanned.
							throw IndexScanCompletedException();
						}

            // Update the parameters for the index since leaf page is invalid
            nextEntry = 0;
            currentPageNum = rightSibPageNo;
            bufMgr->readPage(file, currentPageNum, currentPageData);
					}

          // Check lower limit of scan with entry key. Skip entry if too small.
          if ((lowOp == GT && currentNode->keyArray[nextEntry] <= lowValInt) ||
              (lowOp == GTE && currentNode->keyArray[nextEntry] < lowValInt)) {
                nextEntry++;
                // Restart loop to process next entry
                continue;
          }

          // Check upper limit of scan with entry key. Scan is complete if too big.
          if ((highOp == LT && currentNode->keyArray[nextEntry] >= highValInt) ||
              (highOp == LTE && currentNode->keyArray[nextEntry] > highValInt)) {
                throw IndexScanCompletedException();
          }

          // Exit loop since an entry that meets the requirements has been found
          break;
			}

      // Return the record ID of the entry
      outRid = currentNode->ridArray[nextEntry];

      // Update the index of the next entry to be scanned
      nextEntry++;
		}

		// -----------------------------------------------------------------------------
		// BTreeIndex::endScan
		// -----------------------------------------------------------------------------
		//
		void BTreeIndex::endScan() {

		}

}
