# PageRank

An implementation of the Page Rank algorithm using Hadoop (Java).
This was tested using the inputs (available above):
- pagerank_data.txt - A testing example
- hollins.data - A dataset that can be found [here](https://web2.qatar.cmu.edu/~gdicaro/15381/hw/hw4-files/hollins.dat).

## Input Format

The input used in this implementation (inputs) is as follows:
*Note: Each line is 2 values seperated by a space.*
 - First line: NodesCount EdgesCount *(e.g. "5 9")*
 - NC next lines: NodeID NodeURL *(e.g. "1 http://example.com")*
 - EC next lines: NodeID OutlinkToNodeID *(e.g. "1 2")*

## Step 1

- **Mapper:** Reads the initial input file, ignores the first line and the NC next lines and outputs the EC edges `<IntWritable, IntWritable>` (e.g. `<1, 2>`).
- **Reducer:** Receives the EC edges (Each node and an Iterable of its outlinks) and outputs for each node a concatenated list of its outlinks prefixed with an initial page rank (separated by a tab) `<IntWritable, Text>` (e.g. `<1, "1.0 3,2">`). 

## Step 2

This is the most important step in the process. This is ran multiple times since the output of this step will be the same as in the first step.

- **Mapper:** Reads the last output generated (wheither from the first step or from a previous iteration) and outputs for each `[node, rank, "outlink1,outlink2,..."]`:
  - For each outlink it outputs the outlink and `rank / len(outlinks)`: `<Text, Text>` (e.g. `<"3", "0.5">`.
  - The node and its outlinks prefixed with an open bracket: <Text, Text> (e.g. `<"1",  "[3,2">`).
-  **Reducer:** Receives for each node a list of values (the values can be either page ranks or the list of the outlinks), calculates the pagerank by using the formula `(1 - d) + (d * sum(pageRank))` and outputs the same structure as the first step: <Text, Text> `(e.g. <"1", "0.7875 3,2">)`.

This can be hard to understand using sentences, so here is a pseudo-code:
```
Map(Offset, Text):
	node, rank, outlinks = Parse(Text)
	
	if outlinks == None:
		return

	for (outlink in outlinks):
		Write(outlink, rank / len(outlinks))

	Write(node, '[' + outlinks)

Reduce(Node, Text[]):
	outlinks = []
	totalRank = 0

	for (text in Text):
		if text.startswith('['):
			outlinks = text[1:]
		else
			totalRank += text

	totalRank = (1 - 0.85) + (0.85 * totalRank)
	Write(Node, totalRank + '\t' + outlinks)
```

### Step 3

This step basically only needs a mapper and the use of the shuffle&sort phase to print the rankings but can use a Reducer to, for example, print the top N ranked pages.
- **Setup:** Before mapping, we read the input data (the NC nodes lines) and fill a `HashMap` with each node and its corresponding url, this is simply to be able to print the url of the pages in the ranking instead of just the node ids.
- **Mapper:** Receives the last ranks output (node, rank, outlinks) and outputs for each: `<FloatWritable, Text>` (e.g. `<1.6375, "http://www.hollins.edu/">`). We write the rank as the key so that the shuffle&sort phase (using a custom SortComparator) sorts our entries in a descending order (thus not needing a Reducer).

### Testing

 1. Download the sources.
 2. Compile to a .jar file.
 3. Create an `input` folder and put `pagerank_data.txt` in there.
 5. Use the following command `hadoop jar File.jar PageRank /input /output /input/pagerank_data.txt`.

