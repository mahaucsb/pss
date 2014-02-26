package edu.ucsb.cs.preprocessing.hashing;

import java.util.HashMap;
import java.util.Iterator;

public class WordsMap {

	private HashMap<String, Long> wordsMap = new HashMap<String, Long>();
	private long wordsCount = 0;

	public WordsMap() {}

	public void addWord(String word) {
		if (!wordExists(word))
			wordsMap.put(word, wordsCount);
	}

	public long getNumWords() {
		return wordsCount;
	}

	public void removeAllWords() {
		wordsMap.clear();
	}

	public Iterator<String> getWords() {
		return wordsMap.keySet().iterator();
	}

	public Boolean wordExists(String word) {
		return (wordsMap.containsKey(word) ? true : false);
	}

	public long getWordHash(String word) {
		return (wordsMap.get(word));
	}
}
