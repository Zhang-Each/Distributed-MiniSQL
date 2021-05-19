package miniSQL.CATALOGMANAGER;

public class Address implements Comparable<Address> {
    private String fileName; //file name
    private int blockOffset; //block offset in file
    private int byteOffset;  //byte offset in block

    public Address() {
        //do nothing
    }

    public Address(String fileName,int blockOffset,int byteOffset) {
        this.fileName = fileName;
        this.blockOffset = blockOffset;
        this.byteOffset = byteOffset;
    }

    @Override
    public int compareTo(Address address) {
        if(this.fileName.compareTo(address.fileName) == 0) { //first compare file name
            if (this.blockOffset == address.blockOffset) { //then compare block offset
                return this.byteOffset - address.byteOffset; // finally compare byte offset
            } else {
                return this.blockOffset - address.blockOffset;
            }
        } else {
            return this.fileName.compareTo(address.fileName);
        }

    }

    public String getFileName() {
        return this.fileName;
    }
    public int getBlockOffset() {
        return this.blockOffset;
    }
    public int getByteOffset() {
        return this.byteOffset;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    public void setBlockOffset(int blockOffset) {
        this.blockOffset = blockOffset;
    }
    public void setByteOffset(int byteOffset) {
        this.byteOffset = byteOffset;
    }
}
