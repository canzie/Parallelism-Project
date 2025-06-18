package com.vub.pdproject;

public interface Util {

    /**
     * @param c 	Any character
     * @return 		True if and only if c is a white space character (space, tab, new line etc.) or a punctuation mark (,.!?:;)()
     */
    static boolean isWhitespaceOrPunctuationMark(char c){
        return  Character.isWhitespace(c)
                || c == ','
                || c == '.'
                || c == '!'
                || c == '?'
                || c == ':'
                || c == ';'
                || c == '('
                || c == ')';
    }
}
