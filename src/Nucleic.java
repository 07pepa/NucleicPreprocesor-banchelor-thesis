package sequence;

import sequence.inport.RawDataProcessor;

/**
 * Created by Jan Kolomazník on 27.6.17.
 * optimized by josef havranek
 * not redacted
 */
public enum Nucleic {

    A('A', 'T', 'U'),
    B('B'),
    C('C', 'G', 'G'),
    D('D'),
    G('G', 'C', 'C'),
    H('H'),
    K('K'),
    M('M'),
    N('N'),
    R('R'),
    S('S'),
    T('T', 'A'),
    U('U', Nucleic.ASCIINULL, 'A'),
    V('V'),
    W('W'),
    Y('Y'),
    NONE();
    static final private char ASCIINULL = (char) 0;
    private final byte rawByte;//internal raw representation
    private final byte dnaSupplement;
    private final byte rnaSupplement;

    Nucleic() {
        this(ASCIINULL);
    }

    Nucleic(char raw) {
        this(raw, ASCIINULL);
    }

    Nucleic(char raw, char dSupplement) {
        this(raw, dSupplement, ASCIINULL);
    }

    Nucleic(char raw, char dSupplement, char rSupplement) {
        this.rawByte = (byte) raw;
        this.dnaSupplement = (byte) dSupplement;
        this.rnaSupplement = (byte) rSupplement;
    }

    /**
     * slower but safer way to transform ascii char from source that was not converted to internal format
     *
     * @param b ascii letter converted to byte
     * @return nucleic base
     */
    public static Nucleic get(byte b) {
        return getFromInternalFormat(RawDataProcessor.toInternal(b));
    }

    /**
     * Faster way to get nucleic from internal format string
     *
     * @param b byte of internal format data
     * @return nucleic base
     */
    public static Nucleic getFromInternalFormat(byte b) {
        switch (b) {
            case 65:
                return A;
            case 66:
                return B;
            case 67:
                return C;
            case 68:
                return D;
            case 71:
                return G;
            case 72:
                return H;
            case 75:
                return K;
            case 77:
                return M;
            case 78:
                return N;
            case 82:
                return R;
            case 83:
                return S;
            case 84:
                return T;
            case 85:
                return U;
            case 86:
                return V;
            case 87:
                return W;
            case 89:
                return Y;
            default:
                return NONE;
        }
    }

    //save way for getting nucleic from general java char
    public static Nucleic get(char b) {
        switch (Character.toUpperCase(b)) {
            case 'A':
                return A;
            case 'B':
                return B;
            case 'C':
                return C;
            case 'D':
                return D;
            case 'G':
                return G;
            case 'H':
                return H;
            case 'K':
                return K;
            case 'M':
                return M;
            case 'N':
                return N;
            case 'R':
                return R;
            case 'S':
                return S;
            case 'T':
                return T;
            case 'U':
                return U;
            case 'V':
                return V;
            case 'W':
                return W;
            case 'Y':
                return Y;
            default:
                return NONE;
        }
    }


    public boolean isSupplement(Nucleic nucleic, SequenceType type) {
        return (type == SequenceType.DNA)
                ? this.dnaSupplement == nucleic.rawByte
                : this.rnaSupplement == nucleic.rawByte;
    }


    public Nucleic supplement(SequenceType type) {
        return (type == SequenceType.DNA)
                ? getFromInternalFormat(dnaSupplement)
                : getFromInternalFormat(rnaSupplement);
    }

    public byte toByte() {
        return rawByte;
    }

    /**
     * return true if it is not convertible to nucleic
     *
     * @param in ascii letter from internal format string
     * @return true or false
     */
    public static boolean isNotNuclidInInernal(byte in) {
        return getFromInternalFormat(in) == NONE;
    }
}
