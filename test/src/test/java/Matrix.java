/**
 * Created by sgr on 2017/10/23/023.
 */
public class Matrix {
    private String name;
    private double[][] matrix;

    public Matrix(){ }

    public Matrix(String name){
        this.name = name;
    }
    public Matrix(double[][] matrix){
        this.matrix = matrix;
    }
    public Matrix(String name, double[][] matrix){
        this.name = name;
        this.matrix = matrix;
    }

    public Matrix setMatrix(double[][] matrix) {
        this.matrix = matrix;
        return this;
    }

    public Matrix setName(String name) {
        this.name = name;
        return this;
    }

    public Matrix creatMatrix(int row, int col, double ...values){
        if (row <=0 || col <=0){
            return null;
        }
        matrix = new double[row][col];
        int i = 0;
        int j = 0;
        for (double value : values){
            matrix[i][j] = value;
            j++;
            if (j >= col){
                i++;
                j = 0;
            }
            if (i >= row){
                break;
            }
        }
        return this;
    }

    public Matrix multiply(Matrix matrix){
        double[][] a = null;
        double[][] b = null;
        if (this.matrix != null && this.matrix.length > 0 && this.matrix[0].length > 0){
            a = this.matrix;
            if (matrix.matrix != null && matrix.matrix.length > 0 && matrix.matrix[0].length > 0){
                b = matrix.matrix;
            }
        }
        if (a == null || b == null){
            return null;
        }
        if (a[0].length != b.length){
            return null;
        }
        double[][] c = new double[a.length][b[0].length];
        for (int i=0; i< a.length; i++){
            for (int j=0; j<b[0].length; j++){
                for (int k=0; k<a[0].length; k++){
                    c[i][j] += a[i][k] * b[k][j];
                }
            }
        }
        return new Matrix(c);
    }

    public Matrix addition(Matrix matrix){
        double[][] a = null;
        double[][] b = null;
        if (this.matrix != null && this.matrix.length > 0 && this.matrix[0].length > 0){
            a = this.matrix;
            if (matrix.matrix != null && matrix.matrix.length > 0 && matrix.matrix[0].length > 0){
                b = matrix.matrix;
            }
        }
        if (a == null || b == null){
            return null;
        }
        if (a.length != b.length || a[0].length != b[0].length){
            return null;
        }
        double[][] c = new double[a.length][a[0].length];
        for (int i=0; i<a.length; i++){
            for (int j=0; j<a[0].length; j++){
                c[i][j] = a[i][j] + b[i][j];
            }
        }
        return new Matrix(c);
    }

    public void printMatrix(){
        if (matrix != null && matrix.length > 0 && matrix[0].length > 0){
            System.out.println("Matrix " + this.name + " :");
            for (int i=0; i<matrix.length; i++){
                for (int j=0; j<matrix[0].length; j++){
                    System.out.printf("%-8.2f",matrix[i][j]);
                }
                System.out.println();
            }
            System.out.println("=====================================");
        }
    }

    public static void main(String[] args) {
        Matrix a = new Matrix("a");
        a.creatMatrix(2,2,1,1,1,1);
        a.printMatrix();
        Matrix b = new Matrix("b");
        b.creatMatrix(1,1,3);
        b.printMatrix();
        //matrix.multiply(matrix1).setName("c = a * b").printMatrix();
        Matrix c = new Matrix("c");
        c.creatMatrix(2,2,2,2,2,2);
        c.printMatrix();
        c.addition(a).setName("c + a").printMatrix();

    }

}
