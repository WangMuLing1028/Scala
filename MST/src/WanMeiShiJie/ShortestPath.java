package WanMeiShiJie;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * Created by WJ on 2018/3/20.
 */
public class ShortestPath {
   /* public static void main(String[] args){
        //Scanner in = new Scanner(System.in);
        String FromTo = "1 4";//in.nextLine();
        ArrayList<String> banbens = new ArrayList<>();
        banbens.add("1 3 22");
        banbens.add("1 4 55");
        banbens.add("2 3 12");
        banbens.add("1 2 1");
        banbens.add("3 4 12");

        //getBanben(banbens,in);
        String start = FromTo.split(" ")[0];
        String end = FromTo.split(" ")[1];
        HashMap<String,Integer> out = null;
        System.out.println(findPath(banbens,start,end,out));
    }
    static List<String> getBanben(List<String> s,Scanner in){
        System.out.println("please input n:");
        int n = in.nextInt();
        for (int i = 0 ;i<n;i++){
            System.out.println("please input banben:");
            s.add(in.nextLine());
        }
        return s;
    }
    static HashMap findPath(ArrayList<String> banben, String start, String end, HashMap<String,Integer> output){
        StringBuilder tempOut = new StringBuilder();
        Integer sum = 0;
        while (start!=end) {
            String o = start;
            for (String t : banben
                    ) {
                if (o.equals(t.split(" ")[0])) {
                    tempOut.append(o+"->");
                    sum+=Integer.valueOf(t.split(" ")[2].trim());
                    o = t.split(" ")[1];
                    findPath(banben,o,end,output);
                }
            }
            {

            }
        }
        output.put(tempOut.toString(),sum);
        return output;
    }*/
   public static void main(String[] args) {
       Scanner sc = new Scanner(System.in);
       int n=sc.nextInt();
       int m=sc.nextInt();
       HashMap<Integer, Integer> ma=new HashMap<Integer, Integer>();
       HashMap<Integer, Integer> map=new HashMap<Integer, Integer>();
       map.put(n, 0);
       while(sc.hasNext()){
           int a=sc.nextInt();
           int b=sc.nextInt();
           int c=sc.nextInt();
           Integer in = map.get(b);
           int t=map.get(a)+c;
           if(in==null||in>t){
               map.put(b, t);
               ma.put(b, a);
           }
       }
       Integer in=m;
       ArrayList<Integer> al=new ArrayList<Integer>();
       while((in=ma.get(in))!=null){
           al.add(in);
       }
       for(int i=al.size()-1;i>=0;i--){
           System.out.print(al.get(i));
           System.out.print("->");
       }
       System.out.print(m);
       System.out.println("("+map.get(m)+")");
       sc.close();
   }
}

