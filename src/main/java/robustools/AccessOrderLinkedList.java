package robustools;

import java.util.function.Consumer;

class AccessOrderLinkedList<E extends AccessOrderLinkedList.Node<E>>
{
    public static class Node<E>
    {
        volatile E prev;
        volatile E next;
    }

    private volatile E head;
    private volatile E tail;

    public E addToHead(E newNode)
    {
        linkNewNodeToHead(newNode);
        return newNode;
    }

    public E removeTail()
    {
        E e = this.tail;
        if (e != null) {
            remove(e);
        }
        return e;
    }

    // Not used
    //public E removeHead()
    //{
    //    E e = this.head;
    //    if (e != null) {
    //        remove(e);
    //    }
    //    return e;
    //}

    public void moveToHead(E node)
    {
        remove(node);
        linkNewNodeToHead(node);
    }

    private void linkNewNodeToHead(E newNode)
    {
        if (this.head != null) {
            ((E) this.head).next = newNode;
            newNode.prev = this.head;
        }
        this.head = newNode;
        if (this.tail == null) {
            this.tail = newNode;
        }
    }

    // Not used
    //private void linkNewNodeToTail(E newNode)
    //{
    //    if (this.tail != null) {
    //        this.tail.prev = newNode;
    //        newNode.next = this.tail;
    //    }
    //    this.tail = newNode;
    //}

    public void remove(E node)
    {
        if (node.prev == null) {
            // this node was tail
            this.tail = node.next;
            if (node.next == null) {
                // list is empty
                this.head = null;
            }
            else {
                node.next.prev = null;
                node.next = null;
            }
        }
        else {
            node.prev.next = node.next;
            if (node.next == null) {
                // this node was head
                this.head = node.prev;
            }
            else {
                node.next.prev = node.prev;
                node.next = null;
            }
            node.prev = null;
        }
    }

    public void clear()
    {
        this.head = null;
        this.tail = null;
    }

    public void forEach(Consumer<E> consumer)
    {
        E node = this.head;
        while (node != null) {
            consumer.accept(node);
            node = node.prev;
        }
    }
}
