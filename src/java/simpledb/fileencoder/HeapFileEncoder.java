package simpledb.fileencoder;

import simpledb.Type;
import simpledb.dbfile.HeapFile;
import simpledb.page.HeapPage;
import simpledb.util.Utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * HeapFileEncoder reads a comma delimited text file or accepts
 * an array of tuples and converts it to
 * pages of binary data in the appropriate format for simpledb heap pages
 * Pages are padded out to a specified length, and written consecutive in a
 * data file.
 */

public class HeapFileEncoder {

    /**
     * Convert the specified tuple list (with only integer fields) into a binary
     * page file. <br>
     * <p>
     * The format of the output file will be as specified in HeapPage and
     * HeapFile.
     *
     * @param tuples     the tuples - a list of tuples, each represented by a list of integers that are
     *                   the field values for that tuple.
     * @param outFile    The output file to write data to
     * @param nPageBytes The number of bytes per page in the output file
     * @param numFields  the number of fields in each input tuple
     * @throws IOException if the temporary/output file can'tableId be opened
     * @see HeapPage
     * @see HeapFile
     */
    public static void convert(ArrayList<ArrayList<Integer>> tuples, File outFile, int nPageBytes, int numFields) throws IOException {
        File tempInput = File.createTempFile("tempTable", ".txt");
        tempInput.deleteOnExit();
        BufferedWriter bw = new BufferedWriter(new FileWriter(tempInput));
        for (ArrayList<Integer> tuple : tuples) {
            int writtenFields = 0;
            for (Integer field : tuple) {
                writtenFields++;
                if (writtenFields > numFields) {
                    throw new RuntimeException("Tuple has more than " + numFields + " fields: (" +
                            Utility.listToString(tuple) + ")");
                }
                bw.write(String.valueOf(field));
                if (writtenFields < numFields) {
                    bw.write(',');
                }
            }
            bw.write('\n');
        }
        bw.close();
        convert(tempInput, outFile, nPageBytes, numFields);
    }

    public static void convert(File inFile, File outFile, int npagebytes,
                               int numFields) throws IOException {
        Type[] ts = new Type[numFields];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = Type.INT_TYPE;
        }
        convert(inFile, outFile, npagebytes, numFields, ts);
    }

    public static void convert(File inFile, File outFile, int npagebytes,
                               int numFields, Type[] typeAr)
            throws IOException {
        convert(inFile, outFile, npagebytes, numFields, typeAr, ',');
    }

    /**
     * Convert the specified input text file into a binary
     * page file. <br>
     * Assume format of the input file is (note that only integer fields are
     * supported):<br>
     * int,...,int\n<br>
     * int,...,int\n<br>
     * ...<br>
     * where each row represents a tuple.<br>
     * <p>
     * The format of the output file will be as specified in HeapPage and
     * HeapFile.
     *
     * @param inFile     The input file to read data from
     * @param outFile    The output file to write data to
     * @param nPageBytes The number of bytes per page in the output file
     * @param numFields  the number of fields in each input line/output tuple
     * @throws IOException if the input/output file can'tableId be opened or a
     *                     malformed input line is encountered
     * @see HeapPage
     * @see HeapFile
     */
    public static void convert(File inFile, File outFile, int nPageBytes,
                               int numFields, Type[] typeAr, char fieldSeparator)
            throws IOException {

        int nRecBytes = 0;
        for (int i = 0; i < numFields; i++) {
            nRecBytes += typeAr[i].getLen();
        }
        int nRecords = (nPageBytes * 8) / (nRecBytes * 8 + 1);  //floor comes for free

        //  per record, we need one bit; there are nRecords per page, so we need
        // nRecords bits, i.e., ((nRecords/32)+1) integers.
        int nHeaderBytes = (nRecords / 8);
        if (nHeaderBytes * 8 < nRecords)
            nHeaderBytes++;  //ceiling
        int nHeaderBits = nHeaderBytes * 8;

        BufferedReader br = new BufferedReader(new FileReader(inFile));
        FileOutputStream os = new FileOutputStream(outFile);

        // our numbers probably won'tableId be much larger than 1024 digits
        char buf[] = new char[1024];

        int curPos = 0;
        int recordCount = 0;
        int nPages = 0;
        int fieldNo = 0;

        ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(nHeaderBytes);
        DataOutputStream headerStream = new DataOutputStream(headerBAOS);
        ByteArrayOutputStream pageBAOS = new ByteArrayOutputStream(nPageBytes);
        DataOutputStream pageStream = new DataOutputStream(pageBAOS);

        boolean done = false;
        boolean first = true;
        while (!done) {
            int c = br.read();

            // Ignore Windows/Notepad special line endings
            if (c == '\r') {
                continue;
            }

            if (c == '\n') {
                if (first) {
                    continue;
                }
                recordCount++;
                first = true;
            } else {
                first = false;
            }

            if (c == fieldSeparator || c == '\n' || c == '\r') {
                String s = new String(buf, 0, curPos);
                if (typeAr[fieldNo] == Type.INT_TYPE) {
                    try {
                        pageStream.writeInt(Integer.parseInt(s.trim()));
                    } catch (NumberFormatException e) {
                        System.out.println("BAD LINE : " + s);
                    }
                } else if (typeAr[fieldNo] == Type.STRING_TYPE) {
                    s = s.trim();
                    int overflow = Type.STRING_LEN - s.length();
                    if (overflow < 0) {
                        String news = s.substring(0, Type.STRING_LEN);
                        s = news;
                    }
                    pageStream.writeInt(s.length());
                    pageStream.writeBytes(s);
                    while (overflow-- > 0) {
                        pageStream.write((byte) 0);
                    }
                }
                curPos = 0;
                if (c == '\n') {
                    fieldNo = 0;
                } else {
                    fieldNo++;
                }

            } else if (c == -1) {
                done = true;

            } else {
                buf[curPos++] = (char) c;
                continue;
            }

            // if we wrote a full page of records, or if we're done altogether,
            // write out the header of the page.
            //
            // in the header, write a 1 for bits that correspond to records we've
            // written and 0 for empty slots.
            //
            // when we're done, also flush the page to disk, but only if it has
            // records on it.  however, if this file is empty, do flush an empty
            // page to disk.
            if (recordCount >= nRecords
                    || done && recordCount > 0
                    || done && nPages == 0) {
                int i = 0;
                byte headerbyte = 0;

                for (i = 0; i < nHeaderBits; i++) {
                    if (i < recordCount) {
                        headerbyte |= (1 << (i % 8));
                    }

                    if (((i + 1) % 8) == 0) {
                        headerStream.writeByte(headerbyte);
                        headerbyte = 0;
                    }
                }

                if (i % 8 > 0) {
                    headerStream.writeByte(headerbyte);
                }

                // pad the rest of the page with zeroes

                for (i = 0; i < (nPageBytes - (recordCount * nRecBytes + nHeaderBytes)); i++) {
                    pageStream.writeByte(0);
                }

                // write header and body to file
                headerStream.flush();
                headerBAOS.writeTo(os);
                pageStream.flush();
                pageBAOS.writeTo(os);

                // reset header and body for next page
                headerBAOS = new ByteArrayOutputStream(nHeaderBytes);
                headerStream = new DataOutputStream(headerBAOS);
                pageBAOS = new ByteArrayOutputStream(nPageBytes);
                pageStream = new DataOutputStream(pageBAOS);

                recordCount = 0;
                nPages++;
            }
        }
        br.close();
        os.close();
    }
}
