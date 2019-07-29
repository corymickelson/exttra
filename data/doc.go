// Package data provides the primary data structure/building blocks of the exttra tree.
// Exttra represents csv files in a tree type data structure
//
// Given csv:
//	A     | B      | C
//	Test  |One     |10.00
//	Foo   |Two     |12.00
//	Bar   |Three   |23.00
//
// The exttra tree after parsing will look something like the following:
// Please note the diagram below is just for visualizing how the data is organized,
// the actual tree contains much more information, see [data.node] for the actual representation of a node.
//
//                            / Cell at row index 0 {id={0,0}, value="Test"}
//                          /
//                _ Column A ----- Cell at row index 1 {id={0,1}, value="Foo"}
//              / col at 0 \
//	          /             \ Cell at row index 2 {id={0,2}, value="Bar"}
//          /
//	      /                  / Cell at row index 0 {id={1,0}, value="One"}
//	    /                  /
//	root  ---------Column B--- Cell at row index 1 {id={1,1}, value="Two"}
//	    \         col at 1\
//	     \                 \ Cell at row index 2 {id={1,2}, value="Three"}
//	      \
//	       \
//	        \             / Cell at row index 0 {id={2,0}, value=10.00}
//	         \          /
//	          \ Column C--- Cell at row index 1 {id={2,1}, value=12.00}
//	          col at 2 \
//	                    \ Cell at row index 2 {id={2,2}, value=23.00}
//
//
// This package exposes nodes through the [Composer] and [Editor] interfaces.
//
//
package data
