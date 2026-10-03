// On Stop: flush what we can and disconnect cleanly
const st = context.get('state');
if (st) {
    clearInterval(st.timer);
    if (st.producer && st.connected) {
        return Promise.resolve(st.flush()).then(() => st.producer.disconnect()).catch(e => node.warn(e));
    }
}
