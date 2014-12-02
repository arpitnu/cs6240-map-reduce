/**
 * 
 */
package edu.mapred.finalproject;

/**
 * @author arpitm
 * 
 */
public class FMBitmask implements Comparable<FMBitmask> {
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
	public FMBitmask() {
		for (int i = 0; i < NUM_MASKS; i++) {
			this.bitMasks[i] = (int) 0;
		}
	}
	
	/**
	 * Creates a new FMBitmask object from the input string 
	 * 
	 * @param fmbStr
	 */
	public FMBitmask(String fmbStr) {
//		FMBitmask fmb = new FMBitmask();

		String[] fmbSplits = fmbStr.split(" ");

		for (int i = 0; i < NUM_MASKS; i++) {
			this.bitMasks[i] = Integer.parseInt(fmbSplits[i]);
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
	public int compareTo(FMBitmask fmb) {
		int cmp = 0;

		int[] fmbMasks = fmb.getBitMasks();

		for (int i = 0; i < NUM_MASKS; i++) {
			if (this.bitMasks[i] > fmbMasks[i]) {
				cmp = 1;
				break;
			} else if (this.bitMasks[i] < fmbMasks[i]) {
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
	 * @param fmb
	 */
	public void bitwiseOrWith(FMBitmask fmb) {
		int[] fmbBitMasks = fmb.getBitMasks();
		
		for(int i = 0; i < NUM_MASKS; i++) {
			this.bitMasks[i] = this.bitMasks[i] | fmbBitMasks[i];
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
