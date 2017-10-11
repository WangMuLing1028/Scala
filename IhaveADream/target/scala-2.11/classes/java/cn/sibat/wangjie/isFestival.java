//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package cn.sibat.wangjie;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


public class isFestival {
    public static Map<String, String> mapFestival = new HashMap();
    public static isFestival ih = new isFestival();

    public isFestival() {
    }


    public static void main(String[] args) {
        String mapFestival = GetMapFestival("2016-12-16");
        System.out.println(mapFestival);
    }

    public static String GetMapFestival(String key) {
        return mapFestival.containsKey(key)?(((String)mapFestival.get(key)).matches("1")?"workday":(((String)mapFestival.get(key)).matches("2")?"weekday":"holiday")):"-1";
    }

    public void ReadFestival() throws IOException {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("festival.csv");
        Scanner scan = new Scanner(is, "UTF-8");
        scan.nextLine();

        while(scan.hasNext()) {
            String[] line = scan.nextLine().split(",");
            if(line.length == 2) {
                mapFestival.put(line[0], line[1]);
            } else {
                System.out.println(line[0]);
            }
        }

        is.close();
    }

    static {
        try {
            ih.ReadFestival();
        } catch (IOException var1) {
            var1.printStackTrace();
        }

    }
}
