/**
 * 
 */
package edu.mapred.finalproject;

/**
 * @author arpitm
 * 
 */
public class NodeBitmask implements Comparable<NodeBitmask> {
	/**
	 * Number of bytes in FMBitmask
	 */
	private static final int NUM_MASKS = 24;

	/**
	 * The bitmasks array. 6 bytes ->
	 */
	private int[] bitMasks = new int[NUM_MASKS];

	/**
	 * Default constructor
	 */
	public NodeBitmask() {
		for (int i = 0; i < NUM_MASKS; i++) {
			this.bitMasks[i] = (int) 0;
		}
	}
	
	/**
	 * Creates a new FMBitmask object from the input string 
	 * 
	 * @param nbStr
	 */
	public NodeBitmask(String nbStr) {
//		FMBitmask nb = new FMBitmask();

		String[] nbSplits = nbStr.split(" ");

		for (int i = 0; i < NUM_MASKS; i++) {
			this.bitMasks[i] = Integer.parseInt(nbSplits[i]);
		}
	}

	@Override
	public String toString() {
		StringBuilder retSb = new StringBuilder();

		for (int i = 0; i < NUM_MASKS; i++) {
			if (i == (NUM_MASKS - 1)) {
				retSb.append(this.bitMasks[i]);
			} else {
				retSb.append(this.bitMasks[i]).append(" ");
			}
		}

		String retStr = retSb.toString();

		return retStr;
	}

	@Override
	public int compareTo(NodeBitmask nb) {
		int cmp = 0;

		int[] nbMasks = nb.getBitMasks();

		for (int i = 0; i < NUM_MASKS; i++) {
			if (this.bitMasks[i] > nbMasks[i]) {
				cmp = 1;
				break;
			} else if (this.bitMasks[i] < nbMasks[i]) {
				cmp = -1;
				break;
			} else {
				cmp = 0;
			}
		}

		return cmp;
	}
	
	public void setBit(int bit) {
		if(bit < (NUM_MASKS * 16)) {
			int bmIndex = (NUM_MASKS - 1 - (bit / 16));
			int bmBitIndex = (char) (bit % 16);
			
			int bm = this.bitMasks[bmIndex];
			bm = (bm | (1 << bmBitIndex));
			this.bitMasks[bmIndex] = bm;
		}
		else {
			System.err.println("Set Bit Error in FMBitmask. Bit = " + bit);
			System.exit(-2);
		}
	}
	
	/**
	 * Perform bitwise OR operation with input bitmask
	 * 
	 * @param nb
	 */
	public void bitwiseOrWith(NodeBitmask nb) {
		int[] nbBitMasks = nb.getBitMasks();
		
		for(int i = 0; i < NUM_MASKS; i++) {
			this.bitMasks[i] = this.bitMasks[i] | nbBitMasks[i];
		}
	}
	
	public int getSetBitsCount() {
		int numSetBits = 0;
		
		for(int i = 0; i < NUM_MASKS; i++) {
			int currMask = this.bitMasks[i];
			int lsbMask = 1;
			
			for(int j = 0; j < 16; j++) {
				if(((currMask >> j) & lsbMask) == (int) 1) {
					numSetBits += 1;
				}
			}
		}
		
		return numSetBits;
	}

	/*
	 * Get & Set functions
	 */
	public int[] getBitMasks() {
		return bitMasks;
	}

	public void setBitMasks(int[] bitMasks) {
		this.bitMasks = bitMasks;
	}

	

}
