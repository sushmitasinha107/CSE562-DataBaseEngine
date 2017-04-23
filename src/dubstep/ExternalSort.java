package dubstep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

public class ExternalSort {

	// we divide the file into small blocks. If the blocks
	// are too small, we shall create too many temporary files.
	// If they are too big, we shall be using too much memory.
	public static long estimateBestSizeOfBlocks(File filetobesorted) {
		long sizeoffile = filetobesorted.length();
		// we don't want to open up much more than 1024 temporary files, better
		// run
		// out of memory first. (Even 1024 is stretching it.)
		final int MAXTEMPFILES = 1024;
		long blocksize = sizeoffile / MAXTEMPFILES;
		// on the other hand, we don't want to create many temporary files
		// for naught. If blocksize is smaller than half the free memory, grow
		// it.
		long freemem = Runtime.getRuntime().freeMemory();
		if (blocksize < freemem / 2)
			blocksize = freemem / 2;
		else {
			if (blocksize >= freemem)
				System.err.println("We expect to run out of memory. ");
		}
		return blocksize;
	}

	// This will simply load the file by blocks of x rows, then
	// sort them in-memory, and write the result to a bunch of
	// temporary files that have to be merged later.
	//
	// @param file some flat file
	// @return a list of temporary flat files

	public static List<File> sortInBatch(File file, Comparator<String> cmp , int idx, String opFileakaCOLNAME) throws IOException {
		List<File> files = new ArrayList<File>();
		BufferedReader fbr = new BufferedReader(new FileReader(file));
		long blocksize = estimateBestSizeOfBlocks(file);// in bytes
		try {
			List<String> tmplist = new ArrayList<String>();
			String line = "";
			try {
				while (line != null) {
					long currentblocksize = 0;// in bytes
					while ((currentblocksize < blocksize) && ((line = fbr.readLine()) != null)) { // as
																									// long
																									// as
																									// you
																									// have
																									// 2MB
						tmplist.add(line);
						currentblocksize += line.length(); // 2 + 40; // java
															// uses 16 bits per
															// character + 40
															// bytes of overhead
															// (estimated)
					}
					files.add(sortAndSave(tmplist, cmp,idx,opFileakaCOLNAME));
					tmplist.clear();
				}
			} catch (EOFException oef) {
				if (tmplist.size() > 0) {
					files.add(sortAndSave(tmplist, cmp , idx,opFileakaCOLNAME));
					tmplist.clear();
				}
			}
		} finally {
			fbr.close();
		}
		return files;
	}

	public static File sortAndSave(List<String> tmplist, Comparator<String> cmp , int idx, String opFileakaCOLNAME) throws IOException {
		
		TreeMap map = new  TreeMap();
		
		for(String newRow : tmplist){
			//sort file using treemap

			
			//we have to find key(col for sorting) and put row for it
			
			String values[] = newRow.split("\\|");
			
			String ptype = Main.columnDataTypeMapping.get(opFileakaCOLNAME);
			Main.SQLDataType ptype1 = Main.SQLDataType.valueOf(ptype);
			
			//i know data type of my key. Put row as value
			
			
			List<String> list = new ArrayList();
			if (ptype1 == Main.SQLDataType.sqlint) {

				int key = Integer.parseInt(values[idx]);
				if (map.containsKey(key)) {
					list  = (List<String>) map.get(key);
					list.add(newRow);
					map.put(key, list);
				} else {
					list = new ArrayList<String>();
					list.add(newRow);
					map.put(key, list);
				}

			} else if (ptype1 == Main.SQLDataType.DECIMAL || ptype1 == Main.SQLDataType.decimal) {
				Double key = Double.parseDouble(values[idx]);
				if (map.containsKey(key)) {
					list = (List<String>) map.get(key);
					list.add(newRow);
					map.put(key, list);
				} else {
					list = new ArrayList<String>();

					list.add(newRow);
					map.put(key, list);
				}
			} else {
				// (ptype1 == Main.SQLDataType.string)

				String key = values[idx];
				if (map.containsKey(key)) {
					list = (List<String>) map.get(key);
					list.add(newRow);
					map.put(key, list);
				} else {
					list = new ArrayList<String>();
					list.add(newRow);
					map.put(key, list);
				}
			}
			
		}
		
		//Collections.sort(tmplist, cmp); //
		File newtmpfile = File.createTempFile("sortInBatch", "flatfile");
		newtmpfile.deleteOnExit();
		BufferedWriter fbw = new BufferedWriter(new FileWriter(newtmpfile));
		try {
			
			Iterator iterator = map.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry entry = (Entry) iterator.next();
				for (String rowString : (ArrayList<String>) entry.getValue()) {

					fbw.write(rowString);

					fbw.write('\n');
				
				}
			}
			
			//lets just iterate over this map like old time and flush sorted file 
			/*for (String r : tmplist) {
				fbw.write(r);
				fbw.newLine();
			}*/
		} finally {
			fbw.close();
		}
		return newtmpfile;
	}

	// This merges a bunch of temporary flat files
	// @param files
	// @param output file
	// @return The number of lines sorted. (P. Beaudoin)

	public static int mergeSortedFiles(List<File> files, File outputfile, final Comparator<String> cmp, int idx)
			throws IOException {
		PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(5 ,
				new Comparator<BinaryFileBuffer>() {
					public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
						int x = cmp.compare(i.peek(), j.peek());
						if(x == 0){
							
							
							
							return ( i.fno - j.fno) ;
						}
						return x;
						
					}
				});
		
		TreeMap mapFinal = new  TreeMap();
		
		
		
		int i = 0;
		for (File f : files) {
			BinaryFileBuffer bfb = new BinaryFileBuffer(f);
			bfb.fno = i;
			i++;
			
			pq.add(bfb);
		}
		BufferedWriter fbw = new BufferedWriter(new FileWriter(outputfile));
		int rowcounter = 0;
		try {
			while (pq.size() > 0) {
				BinaryFileBuffer bfb = pq.poll();
				
				
				String r = bfb.pop();
				fbw.write(r);
				fbw.newLine();
				++rowcounter;
				
				
				while (true) {

					if (!bfb.empty()) {
						String s = bfb.peek();

						String a1Arr[] = r.split("\\|");
						String a2Arr[] = s.split("\\|");
						if (a1Arr[idx].compareTo(a2Arr[idx]) == 0) {
							r = bfb.pop();
							fbw.write(r);
							fbw.newLine();
							++rowcounter;
						} else {
							break;
						}
					}
					else{
						break;
					}
				}
				
				if (bfb.empty()) {
					bfb.fbr.close();
					bfb.originalfile.delete();// we don't need you anymore
				} else {
					pq.add(bfb); // add it back
					//remove all buffera
				}
			}
		} finally {
			fbw.close();
			for (BinaryFileBuffer bfb : pq)
				bfb.close();
		}
		return rowcounter;
	}

	public static void onDiskSort(int idx, String opFile) throws IOException {
		// if(args.length<2) {
		// System.out.println("please provide input and output file names");
		// return;
		// }
		String inputfile = "LINEITEM.csv";
		String outputfile = opFile;
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		Comparator<String> comparator = new Comparator<String>() {
			public int compare(String a1, String a2) {
				String a1Arr[] = a1.split("\\|");
				String a2Arr[] = a2.split("\\|");
					return a1Arr[idx].compareTo(a2Arr[idx]);
			}
		};
		List<File> l = sortInBatch(new File("data/" + inputfile), comparator, idx , opFile);
		
		System.out.println(l);
		mergeSortedFiles(l, new File("data/" + outputfile + ".csv"), comparator, idx);
	}
}

class BinaryFileBuffer {
	public static int BUFFERSIZE = 2048;
	public BufferedReader fbr;
	public File originalfile;
	private String cache;
	private boolean empty;

	public int fno;
	public BinaryFileBuffer(File f) throws IOException {
		originalfile = f;
		fbr = new BufferedReader(new FileReader(f), BUFFERSIZE);
		reload();
	}

	public boolean empty() {
		return empty;
	}

	private void reload() throws IOException {
		try {
			if ((this.cache = fbr.readLine()) == null) {
				empty = true;
				cache = null;
			} else {
				empty = false;
			}
		} catch (EOFException oef) {
			empty = true;
			cache = null;
		}
	}

	public void close() throws IOException {
		fbr.close();
	}

	public String peek() {
		if (empty())
			return null;
		return cache;
	}

	public String pop() throws IOException {
		String answer = peek();
		reload();
		return answer;
	}

}
