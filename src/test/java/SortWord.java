public class SortWord {
    public static void main(String[] args) {
         char []arr={'a','b','c','d'};
         for(int i=0;i<arr.length;i++){
             for(int j=i+1;j<arr.length;j++){
                 System.out.println(arr[i]+" "+arr[j]);
             }
         }
    }
}
