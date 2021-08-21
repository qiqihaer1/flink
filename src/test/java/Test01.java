public class Test01 {


    public static void main(String[] args) {
        Bloom bloom = new Bloom(1 << 28);
        Long aa302Q = bloom.hash("AA302Q", 61);
        System.out.println(aa302Q);

    }


}


 class Bloom {
    //布隆过滤器的默认大小是32M
    //32 * 1024 * 1024 * 8
    //2^5  2^10   2^10 * 2^3
    //1后面28个0
    int size;
    long cap;


    public Bloom() {

    }

    public Bloom(int size) {
        this.size = size;
        this.cap = size > 0 ? (long)size : 1 << 28;
    }

    //定义hash函数的结果，当做位图的offset
    public Long hash(String value, Integer seed) {
        Long result = 0L;

        for (int i = 0; i < value.length(); i++) {
            //各种方法去实现都行
            result += result * seed + value.charAt(i);
        }
        //他们之间进行&运算结果一定在位图之间
        long l = result & (cap - 1);//0后面28个1
        return l;
    }
}
