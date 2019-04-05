import java.util.HashMap;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/4/417:40
 * PACKAGE_NAMEpyg
 */
public class demo {
    public static void main(String[] args) {
            int i = 0;
            String s = "select * from a in (";
        HashMap hashMap = new HashMap();
        while (true){
            if (i == 10){
                hashMap.put("i",s);
                s = s = "select * from a in (";
                i = 0;
            }
            s += i+",";
            System.out.println(hashMap);
            i ++;
        }

    }
}
