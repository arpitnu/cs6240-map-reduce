/**
 * 
 */
package edu.mapred.finalproject;

/**
 * @author arpitm
 *
 */
public class FMBitmask {
	/**
	 * Number of bytes in FMBitmask
	 */
	private static final int FMBITMASK_NUM_BYTES = 48;
	
	/**
	 * The bitmasks array. 6 bytes -> 
	 */
	private char[] bitMasks = new char[FMBITMASK_NUM_BYTES];

	/**
	 * Default constructor
	 */
	public FMBitmask() {
		for(int i = 0; i < FMBITMASK_NUM_BYTES; i++) {
			this.bitMasks[i] = (char) 0;
		}
	}

	public static FMBitmask createBitMask(String fmbStr) {
		FMBitmask fmb = new FMBitmask();
		
		String[] fmbSplits = fmbStr.split(" ");
		int len = fmbSplits.length;
		
		for(int i = len - 1; i >= 0; --i) {
			
		}
		
		return fmb;
	}
	
	/*
	 * Get & Set functions
	 */
	public char[] getBitMasks() {
		return bitMasks;
	}

	public void setBitMasks(char[] bitMasks) {
		this.bitMasks = bitMasks;
	}

}
