/*�α�*/
-- �����ͱ���
-- �ؼ��֣� cursor 
-- ���Դ�Ŷ�����󣬶��м�¼��
-- ���emp��������Ա��������
declare
    cursor c1 is select *
                 from emp; --�����α�c1 �������е�emp���е���Ϣ
    emprow emp%rowtype; --������¼����(��)����������ѭ��������ÿ������
begin
    open c1;
    loop
        -- fetch ����ѯ�������������г�ȡһ�и�ֵ��emprow
        fetch c1 into emprow; --��ȡһ����¼(��)����emprow������
        exit when c1%notfound; --���α�(�����)�м�¼Ϊ��ʱ�˳�ѭ��
        dbms_output.put_line(emprow.ename);
    end loop;
    close c1;
end;
