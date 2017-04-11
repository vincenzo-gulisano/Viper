/*  Copyright (C) 2015  Ioannis Nikolakopoulos,  
 * 			Daniel Cederman, 
 * 			Vincenzo Gulisano,
 * 			Marina Papatriantafilou,
 * 			Philippas Tsigas
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  Contact: Ioannis (aka Yiannis) Nikolakopoulos ioaniko@chalmers.se
 *  	     Vincenzo Gulisano vincenzo.gulisano@chalmers.se
 *
 */

package scalegate;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLongArray;

import utilities.ExpBackOff;

public class ScaleGateAArrImpl implements ScaleGate {

	final int maxlevels;
	SGNodeAArrImpl head;
	final SGNodeAArrImpl tail;

	final int numberOfWriters;
	final int numberOfReaders;
	// Arrays of source/reader id local data
	WriterThreadLocalData[] writertld;
	ReaderThreadLocalData[] readertld;


	/* 
	 * maxlevels: maximum levels the skiplist may have (3 or 4) has shown to be working well.
	 * writers: number of writer threads/streams inserting tuples
	 * readers: number of reader threads/streaming reading tuples
	 * Flow Control parameters (optional):
	 * watermark_frequency: writers will add a watermark tuple for every watermark_frequency tuples inserted
	 * watermark_max_defference: a writer will allow at most watermark_max_difference watermarks distance 
	 * from the last inserted to the lowest watermark a reader has seen
	 */
	public ScaleGateAArrImpl (int maxlevels, int writers, int readers, int watermark_frequency, int watermark_max_difference) {
		this.maxlevels = maxlevels;

		this.head = new SGNodeAArrImpl(maxlevels, null, null, -1);
		this.tail = new SGNodeAArrImpl(maxlevels, null, null, -1);

		for (int i = 0; i < maxlevels; i++)
			head.setNext(i, tail);

		this.numberOfWriters = writers;
		this.numberOfReaders = readers;

		writertld = new WriterThreadLocalData[numberOfWriters];
		for (int i=0; i < numberOfWriters; i++) {
			writertld[i] = new WriterThreadLocalData(head, watermark_frequency,watermark_max_difference);
		}

		readertld = new ReaderThreadLocalData[numberOfReaders];
		for (int i=0; i< numberOfReaders; i++) {
			readertld[i] = new ReaderThreadLocalData(head);
		}

		head = null;
	}
	
	public ScaleGateAArrImpl (int maxlevels, int writers, int readers) {
		this(maxlevels, writers, readers, -1, Integer.MAX_VALUE);
	}
	
	@Override
	/*
	 * (non-Javadoc)
	 * @see plugjoin.prototype.TGate#getNextReadyNode(int)
	 */
	public SGTuple getNextReadyTuple(int readerID) {
		SGNodeAArrImpl next = getReaderLocal(readerID).localHead.getNext(0);

		if (next != tail && !next.isLastAdded()) {
			
			getReaderLocal(readerID).localHead = next;

			/*Watermark code for flow control */
			if (next.getTuple().isWM()) {
				long newWM = next.getTuple().getWM();
				if (newWM > getReaderLocal(readerID).watermarksSeen.get(next.writerID)) 
					getReaderLocal(readerID).watermarksSeen.set(next.writerID, newWM);
				return null; //readers outside should not know about watermarks
			}
			
			return next.getTuple();
		}
		return null;
	}

	@Override
	// Add a tuple 
	public void addTuple(SGTuple tuple, int writerID) {
		WriterThreadLocalData writer = getWriterLocal(writerID);
		long counter = writer.incrementAndGetWMCounter();
		this.internalAddTuple(tuple, writerID);
		//The comparison with -1 is a corner case for when no Flow Control is used and the WATERMARK_FREQUENCY is set to -1
			
		if (counter != -1 && counter == writer.WATERMARK_FREQUENCY) {
			//add new watermark
			long newWatermark = writer.incrementAndGetWatermark();
			this.internalAddTuple(new SGWatermarkTuple(tuple.getTS(), newWatermark), writerID);
			
			//check the difference and wait if needed
			while (true) {
				//get the lowest watermark seen from the readers
				long lowestWM = Long.MAX_VALUE;
				for (int readerID = 0; readerID < this.numberOfReaders; readerID++) {
					long tmp = getReaderLocal(readerID).getWatermark(writerID);
					lowestWM = tmp < lowestWM ? tmp : lowestWM;
				}
				
				if (newWatermark - lowestWM < writer.WM_ALLOWED_DIFFERENCE) {
					return;
				}
				//Add exponential backoff here
				try {
//					Thread.sleep(10);
					writer.expBackOff.backoff();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		}
		
		
		
		
	}

	private void insertNode(SGNodeAArrImpl fromNode, SGNodeAArrImpl newNode, final SGTuple obj, final int level) {
		while (true) {
			SGNodeAArrImpl next = fromNode.getNext(level);
			if (next == tail || next.getTuple().compareTo(obj) > 0) {
				newNode.setNext(level, next);
				if (fromNode.trySetNext(level, next, newNode)) {
					break;
				}
			} else {
				fromNode = next;
			}
		}
	}

	private SGNodeAArrImpl internalAddTuple(SGTuple obj, int inputID) {
		int levels = 1;
		WriterThreadLocalData ln = getWriterLocal(inputID);

		while (ln.rand.nextBoolean() && levels < maxlevels)
			levels++;


		SGNodeAArrImpl newNode = new SGNodeAArrImpl (levels, obj, ln, inputID);
		SGNodeAArrImpl [] update = ln.update;
		SGNodeAArrImpl curNode = update[maxlevels - 1];

		for (int i = maxlevels - 1; i >= 0; i--) {
			SGNodeAArrImpl tx = curNode.getNext(i);

			while (tx != tail && tx.getTuple().compareTo(obj) <= 0) {
				curNode = tx;
				tx = curNode.getNext(i);
			}

			update[i] = curNode;
		}

		for (int i = 0; i < levels; i++) {
			this.insertNode(update[i], newNode, obj, i);
		}

		ln.written = newNode;
		return newNode;
	}

	private WriterThreadLocalData getWriterLocal(int writerID) {
		return writertld[writerID];
	}

	private ReaderThreadLocalData getReaderLocal(int readerID) {
		return readertld[readerID];
	}
	protected class WriterThreadLocalData {
		// reference to the last written node by the respective writer
		volatile SGNodeAArrImpl written; 
		SGNodeAArrImpl [] update;
		final Random rand;
		final ExpBackOff expBackOff;
		final int WATERMARK_FREQUENCY;
		final int WM_ALLOWED_DIFFERENCE;
		private long cur_wm_counter;
		private long cur_watermark;

		public WriterThreadLocalData(SGNodeAArrImpl localHead) {
			this(localHead, -1, Integer.MAX_VALUE);
		}
		
		public WriterThreadLocalData(SGNodeAArrImpl localHead, int wm_freq, int wm_diff) {
			update = new SGNodeAArrImpl[maxlevels];
			written = localHead;
			for (int i=0; i < maxlevels; i++) {
				update[i]= localHead;
			}	    
			rand = new Random();
			expBackOff = new ExpBackOff(2, 15);

			this.WATERMARK_FREQUENCY = wm_freq;
			this.WM_ALLOWED_DIFFERENCE = wm_diff;
			cur_wm_counter = 0;
			cur_watermark = 0;
		}
		
		protected long incrementAndGetWMCounter() {
			this.cur_wm_counter = (cur_wm_counter + 1) % WATERMARK_FREQUENCY;
			if (cur_wm_counter != 0)
				return cur_wm_counter;
			else
				return WATERMARK_FREQUENCY;
		}
		
		protected long incrementAndGetWatermark() {
			this.cur_watermark++;
			return this.cur_watermark;
		}
	}

	protected class ReaderThreadLocalData {
		SGNodeAArrImpl localHead;
		AtomicLongArray watermarksSeen;

		public ReaderThreadLocalData(SGNodeAArrImpl lhead) {
			localHead = lhead;
			watermarksSeen = new AtomicLongArray(numberOfWriters);
		}
		
		protected long getWatermark(int writerID) {
			return watermarksSeen.get(writerID);
		}
	}
}
