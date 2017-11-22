/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <stack>
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
            auto root = (NonLeafNodeInt*) rootPage;
            root->level = 0;
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
            } catch (EndOfFileException& e) {
                // Do nothing. Finished scanning file.
            }

            // Unpin header page and root page as they are no longer in use
            try {
                bufMgr->unPinPage(file, headerPageNum, true);
            } catch (PageNotPinnedException& e) {
                // Do nothing.
            }
            try {
                bufMgr->unPinPage(file, rootPageNum, true);
            } catch (PageNotPinnedException& e) {
                // Do nothing.
            }
        } catch (FileExistsException& e) {  // File exists
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
                } catch (PageNotPinnedException& e) {
                    // Do nothing.
                }
                throw BadIndexInfoException("Error: Existing index metadata does not match parameters passed.");
            }
            // Metatdata matches
            // Unpin header page
            try {
                bufMgr->unPinPage(file, headerPageNum, false);
            } catch (PageNotPinnedException& e) {
                // Do nothing.
            }
        }
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::~BTreeIndex -- destructor
    // -----------------------------------------------------------------------------

    // TODO: sreejita
    BTreeIndex::~BTreeIndex() {
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::insertEntry
    // -----------------------------------------------------------------------------

    // TODO: @sahib, @haylee
    void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
        if (key == nullptr)
            return;

        // Get the root node
        Page *currPage;
        bufMgr->readPage(file, rootPageNum, currPage);
        auto currNode = (NonLeafNodeInt*) currPage;
        LeafNodeInt* dataNode;
        int idx, intKey = *((int*) key);

        // Stack to keep track of all parent nodes in the path to the dataNode
        std::stack<PageId> path;
        path.push(rootPageNum);

        // Traverse the b-tree to find the data node for insertion
        while (true) {
            // Traverse the current level of the tree to get the next page index
            for (idx = 0;
                 idx < INTARRAYNONLEAFSIZE &&
                 currNode->keyArray[idx] != -1 &&
                 currNode->keyArray[idx] < intKey;
                 idx++);
            // Read the next page that contains the next node 1 level deeper in
            // the b-tree
            bufMgr->readPage(file, currNode->pageNoArray[idx], currPage);
            path.push(currNode->pageNoArray[idx]);

            // If the next level is the leaf level, set dataNode and break.
            // Otherwise, Set the current node and continue traversal
            if (currNode->level == 1) {
                dataNode = (LeafNodeInt*) &currPage;
                break;
            } else {
                currNode = (NonLeafNodeInt*) &currPage;
            }
        }

        // Find the index to insert the intKey-record pair
        for (idx = 0;
             idx < INTARRAYLEAFSIZE &&
                dataNode->keyArray[idx] != -1 &&
                dataNode->keyArray[idx] < intKey;
             idx++);

        // Checks if the node contains any empty space for insertion
        if (dataNode->keyArray[INTARRAYLEAFSIZE-1] == -1) {
            int newKey = intKey;
            RecordId newRid = rid;

            // Insert the intKey at position idx and shift everything else right
            for (; dataNode->keyArray[idx] != -1; idx++) {
                int oldKey = dataNode->keyArray[idx];
                RecordId oldRid = dataNode->ridArray[idx];
                dataNode->keyArray[idx] = newKey;
                dataNode->ridArray[idx] = newRid;
                newKey = oldKey;
                newRid = oldRid;
            }
            dataNode->keyArray[idx] = newKey;
            dataNode->ridArray[idx] = newRid;

        } else {

            // To store new page created by a split
            Page* newPage = {};

            // Split the leaf node and copy the middle key upwards in the b-tree
            newPage = splitLeafNode(dataNode, intKey, rid);

            // Read the parent non-leaf node
            bufMgr->readPage(file, path.top(), currPage);
            currNode = (NonLeafNodeInt*) &currPage;

            // Keep splitting parents until a parent has empty space available
            while (currNode->keyArray[INTARRAYNONLEAFSIZE-1] != -1) {
                newPage = splitNonLeafNode(currNode, intKey, newPage->page_number());
                // Unpin the page before popping it from the stack
                bufMgr->unPinPage(file, currPage->page_number(), true);
                path.pop();
                if (!path.empty()) {
                    bufMgr->readPage(file, path.top(), currPage);
                    currNode = (NonLeafNodeInt*) &currPage;
                } else {
                    break;
                }
            }

            // No empty non-leaf node found, so create a new root
            if (path.empty()) {
                Page* rootPage;
                PageId pageId;
                // Allocate a new page for the root node
                bufMgr->allocPage(file, pageId, rootPage);
                // Create the new root node
                auto root = (NonLeafNodeInt*) &rootPage;
                root->level = 0;
                for (int i = 1; i < INTARRAYNONLEAFSIZE; i++) {
                    root->keyArray[i] = -1;
                    root->pageNoArray[i] = Page::INVALID_NUMBER;
                }
                root->pageNoArray[INTARRAYNONLEAFSIZE] = Page::INVALID_NUMBER;
                // Copy the middle key and the page numbers of child nodes
                root->keyArray[0] = intKey;
                root->pageNoArray[0] = currPage->page_number();
                root->pageNoArray[1] = newPage->page_number();
                // Update the root page no of the b-tree
                rootPageNum = pageId;
                // Unpin the new root page and the newly split child node
                bufMgr->unPinPage(file, newPage->page_number(), true);
                bufMgr->unPinPage(file, pageId, true);
            }
        }
    }


    Page* BTreeIndex::splitLeafNode(LeafNodeInt *dataNode, int& intKey, const RecordId rid) {
        // Create and allocate the page (and leaf node)
        Page* page;
        PageId pageId = Page::INVALID_NUMBER;
        bufMgr->allocPage(file, pageId, page);
        auto newLeafNode = (LeafNodeInt*) &page;

        // Initialize the node with default values
        for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
            newLeafNode->keyArray[i] = -1;
            newLeafNode->ridArray[i] = {};
        }

        // Get the middle index value and create sorted key and rid array
        int midIdx = (INTARRAYLEAFSIZE + 1) / 2, prevKey = -1, i, j;
        int keyArr[INTARRAYLEAFSIZE+1];
        RecordId ridArr[INTARRAYLEAFSIZE+1];

        // Create a sorted array of all keys with new key in its position
        for (i = 0, j = 0; i < INTARRAYLEAFSIZE; i++) {
            if (prevKey <= intKey && intKey < dataNode->keyArray[j]) {
                keyArr[i] = intKey;
                ridArr[i] = rid;
                prevKey = dataNode->keyArray[j];
                continue;
            }
            prevKey = keyArr[i] = dataNode->keyArray[j];
            ridArr[i] = dataNode->ridArray[j];
            j++;
        }
        // Special case where the key is the last key in the sorted key list
        if (i == j) {
            keyArr[i] = intKey;
            ridArr[i] = rid;
        }

        // Update keys of dataNode (left split) to the first half of keys
        for (i = 0; i < midIdx; ++i) {
            dataNode->keyArray[i] = keyArr[i];
            dataNode->ridArray[i] = ridArr[i];
        }

        // Update keys of newLeafNode (right split) with second half of keys
        for (i = midIdx; i < INTARRAYLEAFSIZE+1; ++i) {
            newLeafNode->keyArray[i-midIdx] = keyArr[i];
            newLeafNode->ridArray[i-midIdx] = ridArr[i];
            // Invalidate corresponding indices in dataNode as second half of
            // that array is now empty
            dataNode->keyArray[i] = -1;
        }

        // Update page IDs of right siblings
        newLeafNode->rightSibPageNo = dataNode->rightSibPageNo;
        dataNode->rightSibPageNo = pageId;

        return page;
    }


    Page* BTreeIndex::splitNonLeafNode(NonLeafNodeInt* node, int &intKey, const PageId pageId) {
        // Create and allocate the page (and new node)
        Page* page;
        PageId pageId_ = Page::INVALID_NUMBER;
        bufMgr->allocPage(file, pageId_, page);
        auto newNode = (NonLeafNodeInt*) &page;

        // Initialize the node with default values
        for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
            newNode->keyArray[i] = -1;
            newNode->pageNoArray[i] = Page::INVALID_NUMBER;
        }
        newNode->pageNoArray[INTARRAYNONLEAFSIZE] = Page::INVALID_NUMBER;

        // Get the middle index value and create sorted key and rid array
        int midIdx = (INTARRAYNONLEAFSIZE + 1) / 2, prevKey = -1, i, j;
        int keyArr[INTARRAYNONLEAFSIZE+1];
        PageId pageNoArr[INTARRAYNONLEAFSIZE+2];

        // The first page number won't be changed during a split as we always
        // create the new leaf (or non-leaf) node to the right side of the
        // existing node.
        pageNoArr[0] = newNode->pageNoArray[0];

        // Create a sorted array of all keys with new key in its position
        for (i = 0, j = 0; i < INTARRAYNONLEAFSIZE; i++) {
            if (prevKey <= intKey && intKey < newNode->keyArray[j]) {
                keyArr[i] = intKey;
                pageNoArr[i] = pageId;
                prevKey = newNode->keyArray[j];
                continue;
            }
            prevKey = keyArr[i] = newNode->keyArray[j];
            pageNoArr[i+1] = newNode->pageNoArray[i+1];
            j++;
        }
        // Special case where the key is the last key in the sorted key list
        if (i == j) {
            keyArr[i] = intKey;
            pageNoArr[i+1] = pageId;
        }

        node->pageNoArray[0] = pageNoArr[0];
        // Update keys of dataNode (left split) to the first half of keys
        for (i = 0; i < midIdx; ++i) {
            node->keyArray[i] = keyArr[i];
            node->pageNoArray[i+1] = pageNoArr[i+1];
        }

        newNode->pageNoArray[0] = pageNoArr[midIdx+1];
        // Update keys of newNode (right split) with second half of keys
        for (i = midIdx+1; i < INTARRAYNONLEAFSIZE+1; ++i) {
            newNode->keyArray[i-midIdx-1] = keyArr[i];
            newNode->pageNoArray[i-midIdx] = pageNoArr[i+1];
            // Invalidate corresponding indices in node as second half of that
            // array is now empty
            node->keyArray[i-1] = -1;
            node->pageNoArray[i] = Page::INVALID_NUMBER;
        }

        // Set the level of the newly created node
        if (node->level == 1)
            newNode->level = 1;
        else
            newNode->level = 0;

        intKey = keyArr[midIdx];

        return page;
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

    // TODO: sahib
    void BTreeIndex::scanNext(RecordId& outRid) {

    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::endScan
    // -----------------------------------------------------------------------------
    //
    // TODO: sreejita
    void BTreeIndex::endScan() {

    }

}
