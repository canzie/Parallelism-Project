/*
Copyright (c) 2016, Software Languages Lab, Vrije Universiteit Brussel
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
 * Neither the name of the Vrije Universiteit Brussel nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.*/
package com.vub.pdproject.pack;

import com.vub.pdproject.search.QueryEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;


/**
 * Divide-and-conquer parallel pack over an array of integers.
 * 
 * Example from Dan Grossman's lecture notes
 * "A Sophomoric Introduction to Shared-Memory Parallelism and Concurrency"
 * {@link http://www.cs.washington.edu/homes/djg/teachingMaterials/}
 * 
 * @author egonzale
 */
public class Pack extends RecursiveAction{

	private static final long serialVersionUID = 1L;

	private static int SEQUENTIAL_THRESHOLD = 1000;

	@FunctionalInterface
	public static interface Filter<T> {
		public boolean filter(T element);
	}

	final static Filter<Integer> greaterThanTen = (i) -> i > 10;
	final static Filter<Integer> divisibleBySeven = (i) -> (i % 7) == 0;
	
	final ArrayList<QueryEngine.RRecord> input;
	final ArrayList<QueryEngine.RRecord> output;
	final int lo;
	final int hi;
	final int[] bitVector;
	final int[] bitSum;

	public Pack(ArrayList<QueryEngine.RRecord> in, ArrayList<QueryEngine.RRecord> out, int l, int h, int[] bitv, int[] bits) {
		input = in; output =out; lo = l; hi = h; bitVector = bitv; bitSum = bits;
	}

	@Override
	public void compute() {
		if ((hi - lo) < SEQUENTIAL_THRESHOLD) {
			for (int i = lo; i < hi; i++) {
				if (bitVector[i] == 1) {
					output.add(input.get(i));
					//output.set(bitSum[i] - 1, input.get(i));
				}
			}
		}else{
			int mid = (lo + hi)/2;
			Pack left = new Pack(input, output, lo, mid, bitVector, bitSum);
			Pack right = new Pack(input, output, mid, hi, bitVector, bitSum);
			left.fork();
			right.compute();
			left.join();
		}
	}




}
