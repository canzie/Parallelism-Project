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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.List;


public class BitvectorPartition extends RecursiveAction{


	int SEQUENTIAL_THRESHOLD = 100;

	boolean calculate_lesser;

	int lo; 
	int hi;
	List<QueryEngine.RRecord> in;
	int[] out;
	QueryEngine.RRecord pivot;

	public BitvectorPartition(List<QueryEngine.RRecord> input, int[] output, int l,int h, boolean calculate_lesser, QueryEngine.RRecord pivot){
		this.lo=l;
		this.hi=h;
		this.in = input;
		this.out = output;
		this.pivot = pivot;
		this.calculate_lesser = calculate_lesser;
	}


	private Boolean lessThanPivot(QueryEngine.RRecord i) {
		return i.compareTo(pivot) < 0;
	}
	private Boolean greaterThanPivot(QueryEngine.RRecord i) {
		return i.compareTo(pivot) > 0;
	}


	public void compute(){
		if( (hi - lo) < SEQUENTIAL_THRESHOLD) {

			for(int i=lo; i < hi; i++) {

				if (calculate_lesser)
					out[i] = lessThanPivot(in.get(i)) ? 1 : 0;
				else
					out[i] = greaterThanPivot(in.get(i)) ? 1 : 0;
			}
		} else {
			int mid = (hi+lo)/2;
			BitvectorPartition left = new BitvectorPartition(in,out,lo,mid, calculate_lesser, pivot);
			BitvectorPartition right= new BitvectorPartition(in,out,mid,hi, calculate_lesser, pivot);
			left.fork();
			right.compute();
			left.join();
		}
	}


}
