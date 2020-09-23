package util;

public class WC {
    public String word;
    public long frequency;
    public WC() {}
    public WC(String word, long frequency) {
        this.word = word;
        this.frequency = frequency;
    }
    @Override
    public String toString() {
        return "WC " + word + " " + frequency;
    }
}
