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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;

/**
 * Parallel prefix sum using the Java Fork/Join framework.
 * 
 * Example from Dan Grossman's lecture notes
 * "A Sophomoric Introduction to Shared-Memory Parallelism and Concurrency"
 * {@link http://www.cs.washington.edu/homes/djg/teachingMaterials/}
 * 
 * @author egonzale
 */

public class PrefixSum extends RecursiveAction{

	private static final long serialVersionUID = 1L;

	/**
	 * This task computes the first pass, the "up" pass of a parallel prefix.
	 * It builds a binary tree in parallel from bottom (ie. the leaf nodes) and to top
	 * (i.e returns the root). 
	 * Every node holds the sum of the integers in its range [low, high[.
	 * The root of the tree holds the sum for the entire range [O,n[
	 */
	static class BuildTree extends RecursiveTask<Node> {

		private static final long serialVersionUID = 1L;

		private static int SEQUENTIAL_THRESHOLD = 1000;

		int[] input; 
		int lo; 
		int hi;

		public BuildTree(int[] in, int l, int h) {
			input = in; lo = l; hi = h;
		}
		@Override
		protected Node compute() {
			if(hi - lo <= SEQUENTIAL_THRESHOLD) {
				int ans = 0;
				for(int i=lo; i < hi; ++i)
					ans += input[i];
				return new Node(null, null, ans, lo, hi);
			} else{
				int mid = (lo + hi)/2;
				BuildTree left = new BuildTree(input, lo, mid);
				BuildTree right = new BuildTree (input, mid, hi);
				left.fork();
				Node rightNode = right.compute();
				Node leftNode = left.join();
				int sum = leftNode.sum + rightNode.sum;
				return new Node(leftNode, rightNode,sum , lo, hi);
			}
		}
	}

	Node node;
	int fromLeft;
	int[] input;
	int[] output;

	PrefixSum(Node n, int sum, int[] in, int[] out){
		node = n; fromLeft = sum;  input = in; output = out;
	}

	/**
	 * PrefixSum computes the second pass, the "down" pass of a parallel prefix.
	 * It computes the prefix-sum passing "down" as an argument the sum of the array
	 * indices to the left of the node
	 */

	@Override
	protected void compute() {
		if (node.isALeaf()){
			// sequential prefix sum from [lo,hi[
			int lo = node.lo;
			int hi = node.hi;
			output[lo] = fromLeft + input[lo];
			for (int i = lo + 1; i < hi; i++) {
				output[i] = output[i-1] + input[i];
			}
		}else{
			PrefixSum left = new PrefixSum(node.left, fromLeft, input, output);
			PrefixSum right = new PrefixSum(node.right, fromLeft + node.left.sum, input, output);
			left.fork();
			right.compute();
			left.join();
		}	
	}


	final static ForkJoinPool fjPool = ForkJoinPool.commonPool();

	public static int[] prefixSumParallel(int[] input) {
		int[] output = new int[input.length];
		// step 1: "up" pass
		Node root = fjPool.invoke(new BuildTree(input,0,input.length)); 
		// step 2: "down" pass
		fjPool.invoke(new PrefixSum(root, 0, input, output)); 
		return output;
	}


	static int[] prefixSumSequential(int[] input) {
		int[] output = new int[input.length];
		output[0] = input[0];
		for(int i=1; i < input.length; i++)
			output[i] = output[i-1] + input[i];
		return output;
	}


}
