package simpledb;

import simpledb.dbfile.DbFile;
import simpledb.dbfile.DbFileIterator;
import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.fileencoder.HeapFileEncoder;
import simpledb.transaction.TransactionId;
import simpledb.tuple.Tuple;
import simpledb.util.Utility;

import java.io.File;
import java.io.IOException;

public class SimpleDb {

    private static char fieldSeparator = ',';

    private enum Instruction {

        CONVERT("convert"),

        PRINT("print"),

        PARSER("parser");

        private final String value;

        Instruction(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * java -jar dist/simpledb.jar convert data.txt 2 "int,int"
     */
    public static void main(String[] args)
            throws DbException, TransactionAbortedException, IOException {

        final String instruction  = args[0];
        final String filePath     = args[1];
        final int numOfAttributes = args.length > 2 ? Integer.parseInt(args[2]) : 0;
        final String format       = args.length > 3 ? args[3] : "";
        /*
            convert a file
         */
        if (Instruction.CONVERT.getValue().equals(instruction)) {
            try {
                if (args.length < 3 || args.length > 5) {
                    System.err.println("Unexpected number of arguments to convert ");
                    return;
                }
                File sourceTxtFile = new File(filePath);
                File targetDatFile = new File(filePath.replaceAll(".txt", ".dat"));
                Type[] ts = new Type[numOfAttributes];

                if (args.length == 3) {
                    for (int i = 0; i < numOfAttributes; i++) {
                        ts[i] = Type.INT_TYPE;
                    }
                } else {
                    String[] typeStringAr = format.split(",");
                    if (typeStringAr.length != numOfAttributes) {
                        System.err.println("The number of types does not agree with the number of columns");
                        return;
                    }
                    int index = 0;
                    for (String s : typeStringAr) {
                        if (Type.INT_TYPE.getValue().toLowerCase().equals(s.toLowerCase())) {
                            ts[index++] = Type.INT_TYPE;
                        } else if (Type.STRING_TYPE.getValue().toLowerCase().equals(s.toLowerCase())) {
                            ts[index++] = Type.STRING_TYPE;
                        } else {
                            System.err.println("Unknown type " + s);
                            return;
                        }
                    }
                    if (args.length == 5) {
                        fieldSeparator = args[4].charAt(0);
                    }
                }

                HeapFileEncoder.convert(sourceTxtFile, targetDatFile,
                        BufferPool.getPageSize(), numOfAttributes, ts, fieldSeparator);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (Instruction.PRINT.getValue().equals(instruction)) {
            File tableFile = new File(filePath);
            DbFile table = Utility.openHeapFile(numOfAttributes, tableFile);
            TransactionId tid = new TransactionId();
            DbFileIterator it = table.iterator(tid);

            if (null == it) {
                System.out.println("Error: method HeapFile.iterator(TransactionId tid) not yet implemented!");
            } else {
                it.open();
                while (it.hasNext()) {
                    Tuple t = it.next();
                    System.out.println(t);
                }
                it.close();
            }
        } else if (Instruction.PARSER.getValue().equals(instruction)) {
            /*
                Strip the first argument and call the parser
              */
            String[] newArgs = new String[args.length - 1];
            System.arraycopy(args, 1, newArgs, 0, args.length - 1);

            try {
                /*
                    dynamically load Parser -- if it doesn't exist, print error message
                 */
                Class<?> c = Class.forName("simpledb.Parser");
                Class<?> s = String[].class;

                java.lang.reflect.Method m = c.getMethod("main", s);
                m.invoke(null, (java.lang.Object) newArgs);
            } catch (ClassNotFoundException cne) {
                System.out.println("Class Parser not found -- perhaps you are trying to run the parser as a part of lab1?");
            } catch (Exception e) {
                System.out.println("Error in parser.");
                e.printStackTrace();
            }
        } else {
            System.err.println("Unknown command: " + args[0]);
            System.exit(1);
        }
    }
}
