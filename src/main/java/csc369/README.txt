Carson Olander

Request1:
In the first task I used 3 MapReduce jobs. The first computed a join of the 2 input files
using a reduce-side join. I picked a reduce side join because the task was to join based on
an access log file and a file that mapped hostnames to their country of origin.
If we were to use this MapReduce to gain insight on access traffic both files would likely
be too large to fit into memory making a reduce side join the more appropriate choice, though
both work with our small samples. The output was (country, count) The second MapReduce
performed a sum by each country and the output was (country, summed_counts). The third MapReduce
performed a sort by count descending and then by each country alphabetically the output was
(sorted_desc(summed_count), country).

Request2:
In the second task I also used 3 MapReduce jobs. The first job computed a join of the 2 input files
using a reduce-side join again the output was (country, url). The second job computed a sum by each
country url pair the output was (country + url, summed_count). The third job computed a sort by country
alphabetically then by summed counts descending the output was (sorted(country) + url, sorted(summed_count)).

Request3:
In the third task I used 2 MapReduce jobs. THe first job computed a join of the 2 input files using
a reduce-side join again the output was (country, url). The second job sorted the output from the
first job by the url and then appended each of the countries that visited the url in alphabetically
order using a TreeMap structure to keep the ordering.